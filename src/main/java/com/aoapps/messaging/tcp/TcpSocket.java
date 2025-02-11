/*
 * ao-messaging-tcp - Asynchronous bidirectional messaging over TCP sockets.
 * Copyright (C) 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2025  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-messaging-tcp.
 *
 * ao-messaging-tcp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-messaging-tcp is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-messaging-tcp.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.aoapps.messaging.tcp;

import com.aoapps.concurrent.Callback;
import com.aoapps.concurrent.Executor;
import com.aoapps.concurrent.Executors;
import com.aoapps.hodgepodge.io.stream.StreamableInput;
import com.aoapps.hodgepodge.io.stream.StreamableOutput;
import com.aoapps.lang.AutoCloseables;
import com.aoapps.lang.Throwables;
import com.aoapps.lang.io.IoUtils;
import com.aoapps.lang.io.function.IOSupplier;
import com.aoapps.messaging.ByteArray;
import com.aoapps.messaging.Message;
import com.aoapps.messaging.MessageType;
import com.aoapps.messaging.Socket;
import com.aoapps.messaging.base.AbstractSocket;
import com.aoapps.messaging.base.AbstractSocketContext;
import com.aoapps.security.Identifier;
import com.aoapps.tempfiles.TempFileContext;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One established connection over a socket.
 */
public class TcpSocket extends AbstractSocket {

  private static final Logger logger = Logger.getLogger(TcpSocket.class.getName());

  public static final String PROTOCOL = "tcp";

  private final Object sendQueueLock = new Object();
  private Queue<Message> sendQueue;

  private final Executors executors = new Executors();

  private final Object lock = new Object();
  private java.net.Socket socket;
  private StreamableInput in;
  private StreamableOutput out;

  /**
   * Creates a new TCP socket.
   */
  public TcpSocket(
      AbstractSocketContext<? extends AbstractSocket> socketContext,
      Identifier id,
      long connectTime,
      java.net.Socket socket,
      StreamableInput in,
      StreamableOutput out
  ) {
    super(
        socketContext,
        id,
        connectTime,
        socket.getRemoteSocketAddress()
    );
    this.socket = socket;
    this.in = in;
    this.out = out;
  }

  @Override
  @SuppressWarnings("ConvertToTryWithResources")
  public void close() throws IOException {
    try {
      super.close();
    } finally {
      try {
        synchronized (lock) {
          if (socket != null) {
            socket.close();
            socket = null;
            in = null;
            out = null;
          }
        }
      } finally {
        executors.close();
      }
    }
  }

  @Override
  public String getProtocol() {
    return PROTOCOL;
  }

  @Override
  @SuppressWarnings({"UseSpecificCatch", "TooBroadCatch", "ThrowableResultIgnored", "AssignmentToCatchBlockParameter"})
  protected void startImpl(
      Callback<? super Socket> onStart,
      Callback<? super Throwable> onError
  ) throws IllegalStateException {
    synchronized (lock) {
      if (socket == null || in == null || out == null) {
        throw new IllegalStateException();
      }
      executors.getUnbounded().submit(() -> {
        try {
          java.net.Socket mySocket;
          synchronized (lock) {
            mySocket = TcpSocket.this.socket;
          }
          if (mySocket == null) {
            SocketException e = new SocketException("Socket is closed");
            if (onError != null) {
              logger.log(Level.FINE, "Calling onError", e);
              try {
                onError.call(e);
              } catch (ThreadDeath td) {
                throw td;
              } catch (Throwable t) {
                logger.log(Level.SEVERE, null, t);
              }
            } else {
              logger.log(Level.FINE, "No onError", e);
            }
          } else {
            // Handle incoming messages in a Thread, can try nio later
            final Executor unbounded = executors.getUnbounded();
            unbounded.submit(() -> {
              try {
                AtomicReference<TempFileContext> tempFileContextRef = new AtomicReference<>();
                try {
                  IOSupplier<TempFileContext> tempFileContextSupplier = () -> {
                    TempFileContext tempFileContext = tempFileContextRef.get();
                    if (tempFileContext == null) {
                      tempFileContext = new TempFileContext();
                      if (!tempFileContextRef.compareAndSet(null, tempFileContext)) {
                        try {
                          tempFileContext.close();
                        } catch (ThreadDeath td) {
                          throw td;
                        } catch (Throwable t) {
                          logger.log(Level.WARNING, null, t);
                        }
                        tempFileContext = tempFileContextRef.get();
                        assert tempFileContext != null;
                      }
                    }
                    return tempFileContext;
                  };
                  while (true) {
                    StreamableInput myIn;
                    synchronized (lock) {
                      // Check if closed
                      myIn = TcpSocket.this.in;
                      if (myIn == null) {
                        break;
                      }
                    }
                    final int size = myIn.readCompressedInt();
                    List<Message> messages = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                      MessageType type = MessageType.getFromTypeByte(myIn.readByte());
                      int arraySize = myIn.readCompressedInt();
                      byte[] array = new byte[arraySize];
                      IoUtils.readFully(myIn, array, 0, arraySize);
                      messages.add(
                          type.decode(
                              new ByteArray(
                                  array,
                                  arraySize
                              ),
                              tempFileContextSupplier
                          )
                      );
                    }
                    final Future<?> future = callOnMessages(Collections.unmodifiableList(messages));
                    TempFileContext tempFileContext = tempFileContextRef.get();
                    if (tempFileContext != null && tempFileContext.getSize() != 0) {
                      // Close temp file context, thus deleting temp files, once all messages have been handled
                      final TempFileContext closeMeNow = tempFileContext;
                      unbounded.submit(() -> {
                        try {
                          try {
                            // Wait until all messages handled
                            future.get();
                          } finally {
                            try {
                              // Delete temp files
                              closeMeNow.close();
                            } catch (ThreadDeath td) {
                              throw td;
                            } catch (Throwable t) {
                              logger.log(Level.SEVERE, null, t);
                            }
                          }
                        } catch (InterruptedException e) {
                          logger.log(Level.FINE, null, e);
                          // Restore the interrupted status
                          Thread.currentThread().interrupt();
                        } catch (ThreadDeath td) {
                          throw td;
                        } catch (Throwable t) {
                          logger.log(Level.SEVERE, null, t);
                        }
                      });
                      tempFileContextRef.set(null);
                    }
                  }
                } finally {
                  TempFileContext tempFileContext = tempFileContextRef.get();
                  if (tempFileContext != null) {
                    try {
                      tempFileContext.close();
                    } catch (ThreadDeath td) {
                      throw td;
                    } catch (Throwable t) {
                      logger.log(Level.WARNING, null, t);
                    }
                  }
                }
              } catch (ThreadDeath td) {
                try {
                  if (!isClosed()) {
                    callOnError(td);
                  }
                } catch (Throwable t) {
                  @SuppressWarnings("ThrowableResultIgnored")
                  Throwable t2 = Throwables.addSuppressed(td, t);
                  assert t2 == td;
                }
                throw td;
              } catch (Throwable t) {
                if (!isClosed()) {
                  callOnError(t);
                }
              } finally {
                try {
                  close();
                } catch (ThreadDeath td) {
                  throw td;
                } catch (Throwable t) {
                  logger.log(Level.SEVERE, null, t);
                }
              }
            });
          }
          if (onStart != null) {
            logger.log(Level.FINE, "Calling onStart: {0}", TcpSocket.this);
            try {
              onStart.call(TcpSocket.this);
            } catch (ThreadDeath td) {
              throw td;
            } catch (Throwable t) {
              logger.log(Level.SEVERE, null, t);
            }
          } else {
            logger.log(Level.FINE, "No onStart: {0}", TcpSocket.this);
          }
        } catch (Throwable t0) {
          if (onError != null) {
            logger.log(Level.FINE, "Calling onError", t0);
            try {
              onError.call(t0);
            } catch (ThreadDeath td) {
              t0 = Throwables.addSuppressed(td, t0);
              assert t0 == td;
            } catch (Throwable t2) {
              logger.log(Level.SEVERE, null, t2);
            }
          } else {
            logger.log(Level.FINE, "No onError", t0);
          }
          if (t0 instanceof ThreadDeath) {
            throw (ThreadDeath) t0;
          }
        }
      });
    }
  }

  @Override
  @SuppressWarnings({"UseSpecificCatch", "TooBroadCatch", "AssignmentToCatchBlockParameter"})
  protected void sendMessagesImpl(Collection<? extends Message> messages) {
    if (logger.isLoggable(Level.FINEST)) {
      int size = messages.size();
      logger.log(Level.FINEST, "Enqueuing {0} {1}", new Object[]{size, (size == 1) ? "message" : "messages"});
    }
    synchronized (sendQueueLock) {
      // Enqueue asynchronous write
      boolean isFirst;
      if (sendQueue == null) {
        sendQueue = new LinkedList<>();
        isFirst = true;
      } else {
        isFirst = false;
      }
      sendQueue.addAll(messages);
      if (isFirst) {
        logger.log(Level.FINEST, "Submitting runnable");
        // When the queue is first created, we submit the queue runner to the executor for queue processing
        // There is only one executor per queue, and on queue per socket
        executors.getUnbounded().submit(() -> {
          try {
            final List<Message> msgs = new ArrayList<>();
            while (true) {
              StreamableOutput myOut;
              synchronized (lock) {
                // Check if closed
                myOut = TcpSocket.this.out;
                if (myOut == null) {
                  break;
                }
              }
              // Get all of the messages until the queue is empty
              synchronized (sendQueueLock) {
                if (sendQueue.isEmpty()) {
                  logger.log(Level.FINEST, "run: Queue empty, flushing and returning");
                  myOut.flush();
                  // Remove the empty queue so a new executor will be submitted on next event
                  sendQueue = null;
                  break;
                } else {
                  msgs.addAll(sendQueue);
                  sendQueue.clear();
                }
              }
              // Write the messages without holding the queue lock
              final int size = msgs.size();
              if (logger.isLoggable(Level.FINEST)) {
                logger.log(Level.FINEST, "run: Writing {0} {1}", new Object[]{size, (size == 1) ? "message" : "messages"});
              }
              myOut.writeCompressedInt(size);
              for (int i = 0; i < size; i++) {
                Message message = msgs.get(i);
                myOut.writeByte(message.getMessageType().getTypeByte());
                ByteArray data = message.encodeAsByteArray();
                myOut.writeCompressedInt(data.size);
                myOut.write(data.array, 0, data.size);
              }
              msgs.clear();
            }
          } catch (Throwable t0) {
            if (!isClosed()) {
              try {
                callOnError(t0);
              } catch (Throwable t) {
                t0 = Throwables.addSuppressed(t0, t);
              } finally {
                t0 = AutoCloseables.closeAndCatch(t0, this);
              }
            }
            if (t0 instanceof ThreadDeath) {
              throw (ThreadDeath) t0;
            }
            logger.log(Level.SEVERE, null, t0);
          }
        });
      }
    }
  }
}
