/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.streaming.messages.StreamInitMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * ConnectionHandler manages incoming/outgoing message exchange for the {@link StreamSession}.
 *
 * <p>
 * Internally, ConnectionHandler manages thread to receive incoming {@link StreamMessage} and thread to
 * send outgoing message. Messages are encoded/decoded on those thread and handed to
 * {@link StreamSession#messageReceived(StreamMessage, int)}.
 */
public class ConnectionHandler
{
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);
    private static final double SOCKET_ERROR_CHANCE = getSocketErrorChance();

    private final StreamSession session;

    private IncomingMessageHandler incoming;
    private OutgoingMessageHandler outgoing;

    ConnectionHandler(StreamSession session)
    {
        this.session = session;
        this.incoming = new IncomingMessageHandler(session);
        this.outgoing = new OutgoingMessageHandler(session);
    }

    /**
     * Set up incoming message handler and initiate streaming.
     *
     * This method is called once on initiator.
     *
     * @throws IOException
     */
    public void initiate() throws IOException
    {
        logger.info("[Stream #{}] Sending stream init for incoming stream", session.planId());
        Socket incomingSocket = session.createConnection();
        incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION);
        incoming.sendInitMessage(incomingSocket, true);

        logger.info("[Stream #{}] Sending stream init for outgoing stream", session.planId());
        Socket outgoingSocket = session.createConnection();
        outgoing.start(outgoingSocket, StreamMessage.CURRENT_VERSION);
        outgoing.sendInitMessage(outgoingSocket, false);
    }

    /**
     * Set up outgoing message handler on receiving side.
     *
     * @param socket socket to use for {@link org.apache.cassandra.streaming.ConnectionHandler.OutgoingMessageHandler}.
     * @param version Streaming message version
     * @throws IOException
     */
    public void initiateOnReceivingSide(Socket socket, boolean isForOutgoing, int version) throws IOException
    {
        if (isForOutgoing)
            outgoing.start(socket, version);
        else
            incoming.start(socket, version);
    }

    public ListenableFuture<?> close(boolean force)
    {
        logger.info("[Stream #{}] Closing stream connection handler on {}. (force={})",
                    session.planId(), session.peer, force);

        ListenableFuture<?> inClosed = incoming == null ? Futures.immediateFuture(null) : incoming.close(force);
        ListenableFuture<?> outClosed = outgoing == null ? Futures.immediateFuture(null) : outgoing.close(force);

        return Futures.allAsList(inClosed, outClosed);
    }

    /**
     * Enqueue messages to be sent.
     *
     * @param messages messages to send
     */
    public void sendMessages(Collection<? extends StreamMessage> messages)
    {
        for (StreamMessage message : messages)
            sendMessage(message);
    }

    public void sendMessage(StreamMessage message)
    {
        if (outgoing.isClosed())
            throw new RuntimeException("Outgoing stream handler has been closed");

        outgoing.enqueue(message);
    }

    /**
     * @return true if outgoing connection is opened and ready to send messages
     */
    public boolean isOutgoingConnected()
    {
        return outgoing != null && !outgoing.isClosed();
    }

    public boolean isConnected()
    {
        return outgoing != null && incoming !=null && outgoing.isConnected() && incoming.isConnected();
    }

    abstract static class MessageHandler implements Runnable
    {
        protected final StreamSession session;
        protected final int epoch;

        protected int protocolVersion;
        protected Socket socket;

        private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference<>();
        protected boolean forceClose = false;

        protected MessageHandler(StreamSession session)
        {
            this.session = session;
            this.epoch = session.getEpoch();
        }

        protected abstract String name();

        protected static DataOutputStreamAndChannel getWriteChannel(Socket socket) throws IOException
        {
            WritableByteChannel out = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            if (out == null)
                out = Channels.newChannel(socket.getOutputStream());
            return new DataOutputStreamAndChannel(socket.getOutputStream(), out);
        }

        protected static ReadableByteChannel getReadChannel(Socket socket) throws IOException
        {
            ReadableByteChannel in = socket.getChannel();
            // socket channel is null when encrypted(SSL)
            return in == null
                 ? Channels.newChannel(socket.getInputStream())
                 : in;
        }

        public void sendInitMessage(Socket socket, boolean isForOutgoing) throws IOException
        {
            StreamInitMessage message = new StreamInitMessage(
                    FBUtilities.getBroadcastAddress(),
                    session.sessionIndex(),
                    session.planId(),
                    session.description(),
                    isForOutgoing);
            ByteBuffer messageBuf = message.createMessage(false, protocolVersion);
            getWriteChannel(socket).write(messageBuf);
        }

        public void start(Socket socket, int protocolVersion)
        {
            this.socket = socket;
            this.protocolVersion = protocolVersion;

            new Thread(this, name() + "-" + session.peer).start();
        }

        public ListenableFuture<?> close()
        {
            return close(false);
        }

        public ListenableFuture<?> close(boolean force)
        {
            // Assume it wasn't closed. Not a huge deal if we create a future on a race
            SettableFuture<?> future = SettableFuture.create();
            if (closeFuture.compareAndSet(null, future))
            {
                this.forceClose = force;
                return future;
            }
            return closeFuture.get();
        }

        public boolean isClosed()
        {
            return closeFuture.get() != null;
        }

        public boolean isConnected()
        {
            return socket != null && !isClosed();
        }

        protected void signalCloseDone()
        {
            if (closeFuture.get() != null)
                closeFuture.get().set(null);

            // We can now close the socket
            try
            {
                socket.close();
            }
            catch (IOException e)
            {
                // Erroring out while closing shouldn't happen but is not really a big deal, so just log
                // it at DEBUG and ignore otherwise.
                logger.info("Unexpected error while closing streaming connection", e);
            }
        }

        protected void handleError(Throwable t)
        {
            if (isClosed())
                logger.warn("Ignoring error on closed message handler (epoch is {}).", epoch, t);
            else if (isConnectionError(t) && session.state().isPrepared())
            {
                session.onConnectionError(epoch, t);
            }
            else if (t instanceof InterruptedException) {
                throw new AssertionError(t);
            } else {
                logger.info("Error on state {}", session.state());
                JVMStabilityInspector.inspectThrowable(t);
                session.onError(t);
                if (t instanceof SocketException)
                {
                    // socket is closed
                    close();
                }
            }
        }

        public boolean isConnectionError(Throwable t)
        {
            return t instanceof SocketException ||
                   t instanceof SocketTimeoutException ||
                   (t instanceof IOException &&
                        (t.getMessage().contains("Broken pipe") ||
                         t.getMessage().contains("Connection reset")));
        }

        protected void maybeThrowSocketException() throws SocketException
        {
            if (SOCKET_ERROR_CHANCE > 0 &&
                session.state() == StreamSession.State.STREAMING &&
                SOCKET_ERROR_CHANCE > ThreadLocalRandom.current().nextDouble())
                throw new SocketException(String.format("Fake socket error caused by option " +
                                                        "cassandra.test.streaming_socket_error_chance of %.2f",
                                                        SOCKET_ERROR_CHANCE));
        }
    }

    /**
     * Incoming streaming message handler
     */
    static class IncomingMessageHandler extends MessageHandler
    {
        IncomingMessageHandler(StreamSession session)
        {
            super(session);
        }

        protected String name()
        {
            return "STREAM-IN";
        }

        public void run()
        {
            StreamMessage message = null;
            try
            {
                ReadableByteChannel in = getReadChannel(socket);
                while (!isClosed())
                {
                    maybeThrowSocketException();
                    // receive message
                    message = StreamMessage.deserialize(in, protocolVersion, session);
                    // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok
                    // to ignore here since we'll have asked for a retry.
                    if (message != null)
                    {
                        logger.info("[Stream #{}][{}][{}] Received {}", session.planId(), epoch, session.getEpoch(), message);
                        session.messageReceived(message, epoch);
                    }
                }
            }
            catch (Throwable t)
            {
                if (message != null)
                handleError(t);
            }
            finally
            {
                signalCloseDone();
            }
        }
    }

    /**
     * Outgoing file transfer thread
     */
    static class OutgoingMessageHandler extends MessageHandler
    {
        /*
         * All out going messages are queued up into messageQueue.
         * The size will grow when received streaming request.
         *
         * Queue is also PriorityQueue so that prior messages can go out fast.
         */
        private final PriorityBlockingQueue<StreamMessage> messageQueue = new PriorityBlockingQueue<>(64, new Comparator<StreamMessage>()
        {
            public int compare(StreamMessage o1, StreamMessage o2)
            {
                return o2.getPriority() - o1.getPriority();
            }
        });

        OutgoingMessageHandler(StreamSession session)
        {
            super(session);
        }

        protected String name()
        {
            return "STREAM-OUT";
        }

        public void enqueue(StreamMessage message)
        {
            messageQueue.put(message);
        }

        public void run()
        {
            try
            {
                DataOutputStreamAndChannel out = getWriteChannel(socket);

                StreamMessage next;
                while (!isClosed())
                {
                    if ((next = messageQueue.poll(1, TimeUnit.SECONDS)) != null)
                    {
                        logger.info("[Stream #{}][{}][{}] Sending {}", session.planId(), epoch, session.getEpoch(), next);
                        sendMessage(out, next);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            close();
                    }
                }

                // Sends the last messages on the queue
                while (!forceClose && (next = messageQueue.poll()) != null)
                    sendMessage(out, next);
            }
            catch (Throwable e)
            {
                handleError(e);
            }
            finally
            {
                signalCloseDone();
            }
        }

        private void sendMessage(DataOutputStreamAndChannel out, StreamMessage message) throws IOException
        {
            StreamMessage.serialize(message, out, protocolVersion, session);
            if (message instanceof OutgoingFileMessage)
            {
                session.fileSent(((OutgoingFileMessage)message).header, epoch);
            }
        }
    }

    private static double getSocketErrorChance()
    {
        String errorChance = System.getProperty("cassandra.test.streaming_socket_error_chance");
        if (errorChance != null)
        {
            try
            {
                double socketErrorChance = Double.parseDouble(errorChance);
                logger.info("Using streaming_socket_error_chance of {}", socketErrorChance);
                return socketErrorChance;
            }
            catch (NumberFormatException nfe)
            {
                logger.warn("Invalid property streaming_socket_error_chance value: {}", errorChance);
            }
        }
        return 0.0;
    }
}
