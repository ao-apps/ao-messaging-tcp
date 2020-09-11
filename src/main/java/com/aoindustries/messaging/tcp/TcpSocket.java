/*
 * ao-messaging-tcp - Asynchronous bidirectional messaging over TCP sockets.
 * Copyright (C) 2014, 2015, 2016, 2017, 2018, 2019, 2020  AO Industries, Inc.
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
 * along with ao-messaging-tcp.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.messaging.tcp;

import com.aoindustries.concurrent.Callback;
import com.aoindustries.concurrent.Executor;
import com.aoindustries.concurrent.Executors;
import com.aoindustries.io.IoUtils;
import com.aoindustries.io.stream.StreamableInput;
import com.aoindustries.io.stream.StreamableOutput;
import com.aoindustries.lang.Throwables;
import com.aoindustries.messaging.ByteArray;
import com.aoindustries.messaging.Message;
import com.aoindustries.messaging.MessageType;
import com.aoindustries.messaging.Socket;
import com.aoindustries.messaging.base.AbstractSocket;
import com.aoindustries.messaging.base.AbstractSocketContext;
import com.aoindustries.security.Identifier;
import com.aoindustries.tempfiles.TempFileContext;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Future;
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
	public void close() throws IOException {
		try {
			super.close();
		} finally {
			try {
				synchronized(lock) {
					if(socket!=null) {
						socket.close();
						socket = null;
						in = null;
						out = null;
					}
				}
			} finally {
				executors.dispose();
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
		synchronized(lock) {
			if(socket==null || in==null || out==null) throw new IllegalStateException();
			executors.getUnbounded().submit(() -> {
				try {
					java.net.Socket _socket;
					synchronized(lock) {
						_socket = TcpSocket.this.socket;
					}
					if(_socket==null) {
						SocketException e = new SocketException("Socket is closed");
						if(onError != null) {
							logger.log(Level.FINE, "Calling onError", e);
							try {
								onError.call(e);
							} catch(ThreadDeath td) {
								throw td;
							} catch(Throwable t) {
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
								TempFileContext tempFileContext;
								try {
									tempFileContext = new TempFileContext();
								} catch(ThreadDeath td) {
									throw td;
								} catch(Throwable t) {
									logger.log(Level.WARNING, null, t);
									tempFileContext = null;
								}
								try {
									while(true) {
										StreamableInput _in;
										synchronized(lock) {
											// Check if closed
											_in = TcpSocket.this.in;
											if(_in==null) break;
										}
										final int size = _in.readCompressedInt();
										List<Message> messages = new ArrayList<>(size);
										for(int i=0; i<size; i++) {
											MessageType type = MessageType.getFromTypeByte(_in.readByte());
											int arraySize = _in.readCompressedInt();
											byte[] array = new byte[arraySize];
											IoUtils.readFully(_in, array, 0, arraySize);
											messages.add(
												type.decode(
													new ByteArray(
														array,
														arraySize
													),
													tempFileContext
												)
											);
										}
										final Future<?> future = callOnMessages(Collections.unmodifiableList(messages));
										if(tempFileContext != null && tempFileContext.getSize() != 0) {
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
														} catch(ThreadDeath td) {
															throw td;
														} catch(Throwable t) {
															logger.log(Level.SEVERE, null, t);
														}
													}
												} catch(InterruptedException e) {
													logger.log(Level.FINE, null, e);
													Thread.currentThread().interrupt();
												} catch(ThreadDeath td) {
													throw td;
												} catch(Throwable t) {
													logger.log(Level.SEVERE, null, t);
												}
											});
											try {
												tempFileContext = new TempFileContext();
											} catch(ThreadDeath td) {
												throw td;
											} catch(Throwable t) {
												logger.log(Level.WARNING, null, t);
												tempFileContext = null;
											}
										}
									}
								} finally {
									if(tempFileContext != null) {
										try {
											tempFileContext.close();
										} catch(ThreadDeath td) {
											throw td;
										} catch(Throwable t) {
											logger.log(Level.WARNING, null, t);
										}
									}
								}
							} catch(ThreadDeath td) {
								try {
									if(!isClosed()) callOnError(td);
								} catch(Throwable t) {
									Throwables.addSuppressed(td, t);
								}
								throw td;
							} catch(Throwable t) {
								if(!isClosed()) callOnError(t);
							} finally {
								try {
									close();
								} catch(ThreadDeath td) {
									throw td;
								} catch(Throwable t) {
									logger.log(Level.SEVERE, null, t);
								}
							}
						});
					}
					if(onStart != null) {
						logger.log(Level.FINE, "Calling onStart: {0}", TcpSocket.this);
						try {
							onStart.call(TcpSocket.this);
						} catch(ThreadDeath td) {
							throw td;
						} catch(Throwable t) {
							logger.log(Level.SEVERE, null, t);
						}
					} else {
						logger.log(Level.FINE, "No onStart: {0}", TcpSocket.this);
					}
				} catch(Throwable t) {
					if(onError != null) {
						logger.log(Level.FINE, "Calling onError", t);
						try {
							onError.call(t);
						} catch(ThreadDeath td) {
							t = Throwables.addSuppressed(t, td);
						} catch(Throwable t2) {
							logger.log(Level.SEVERE, null, t2);
						}
					} else {
						logger.log(Level.FINE, "No onError", t);
					}
					if(t instanceof ThreadDeath) throw (ThreadDeath)t;
				}
			});
		}
	}

	@Override
	@SuppressWarnings({"UseSpecificCatch", "TooBroadCatch", "AssignmentToCatchBlockParameter"})
	protected void sendMessagesImpl(Collection<? extends Message> messages) {
		if(logger.isLoggable(Level.FINEST)) {
			int size = messages.size();
			logger.log(Level.FINEST, "Enqueuing {0} {1}", new Object[]{size, (size == 1) ? "message" : "messages"});
		}
		synchronized(sendQueueLock) {
			// Enqueue asynchronous write
			boolean isFirst;
			if(sendQueue == null) {
				sendQueue = new LinkedList<>();
				isFirst = true;
			} else {
				isFirst = false;
			}
			sendQueue.addAll(messages);
			if(isFirst) {
				logger.log(Level.FINEST, "Submitting runnable");
				// When the queue is first created, we submit the queue runner to the executor for queue processing
				// There is only one executor per queue, and on queue per socket
				executors.getUnbounded().submit(() -> {
					try {
						final List<Message> msgs = new ArrayList<>();
						while(true) {
							StreamableOutput _out;
							synchronized(lock) {
								// Check if closed
								_out = TcpSocket.this.out;
								if(_out==null) break;
							}
							// Get all of the messages until the queue is empty
							synchronized(sendQueueLock) {
								if(sendQueue.isEmpty()) {
									logger.log(Level.FINEST, "run: Queue empty, flushing and returning");
									_out.flush();
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
							if(logger.isLoggable(Level.FINEST)) {
								logger.log(Level.FINEST, "run: Writing {0} {1}", new Object[]{size, (size == 1) ? "message" : "messages"});
							}
							_out.writeCompressedInt(size);
							for(int i=0; i<size; i++) {
								Message message = msgs.get(i);
								_out.writeByte(message.getMessageType().getTypeByte());
								ByteArray data = message.encodeAsByteArray();
								_out.writeCompressedInt(data.size);
								_out.write(data.array, 0, data.size);
							}
							msgs.clear();
						}
					} catch(Throwable t) {
						if(!isClosed()) {
							try {
								callOnError(t);
							} catch(Throwable t2) {
								t = Throwables.addSuppressed(t, t2);
							} finally {
								try {
									close();
								} catch(Throwable t2) {
									t = Throwables.addSuppressed(t, t2);
								}
							}
						}
						if(t instanceof ThreadDeath) throw (ThreadDeath)t;
						logger.log(Level.SEVERE, null, t);
					}
				});
			}
		}
	}
}
