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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
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
 * {@link StreamSession#messageReceived(org.apache.cassandra.streaming.messages.StreamMessage)}.
 */
public class ConnectionHandler
{

    private static final String FAIL_ADDRESS = System.getProperty("cassandra.test.stream.fail_address");
    private static final String FREEZE_ADDRESS = System.getProperty("cassandra.test.stream.freeze_address");
    private static final Logger logger = LoggerFactory.getLogger(ConnectionHandler.class);

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
        logger.debug("[Stream #{}] Sending stream init for incoming stream", session.planId());
        Socket incomingSocket = session.createConnection();
        incoming.start(incomingSocket, StreamMessage.CURRENT_VERSION);
        incoming.sendInitMessage(incomingSocket, true);

        logger.debug("[Stream #{}] Sending stream init for outgoing stream", session.planId());
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

    public ListenableFuture<?> close()
    {
        logger.debug("[Stream #{}] Closing stream connection handler on {}", session.planId(), session.peer);

        ListenableFuture<?> inClosed = closeIncoming();
        ListenableFuture<?> outClosed = closeOutgoing();

        return Futures.allAsList(inClosed, outClosed);
    }

    public ListenableFuture<?> closeOutgoing()
    {
        return outgoing == null ? Futures.immediateFuture(null) : outgoing.close();
    }

    public ListenableFuture<?> closeIncoming()
    {
        return incoming == null ? Futures.immediateFuture(null) : incoming.close();
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

    abstract static class MessageHandler implements Runnable
    {
        protected final StreamSession session;

        protected int protocolVersion;
        protected Socket socket;

        private final AtomicReference<SettableFuture<?>> closeFuture = new AtomicReference<>();

        protected MessageHandler(StreamSession session)
        {
            this.session = session;
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
            //we do this instead of socket.getChannel() so socketSoTimeout is respected
            return Channels.newChannel(socket.getInputStream());
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
            // Assume it wasn't closed. Not a huge deal if we create a future on a race
            SettableFuture<?> future = SettableFuture.create();
            return closeFuture.compareAndSet(null, future)
                 ? future
                 : closeFuture.get();
        }

        public boolean isClosed()
        {
            return closeFuture.get() != null;
        }

        protected void signalCloseDone()
        {
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
                logger.debug("Unexpected error while closing streaming connection", e);
            }
        }

        protected void checkTestProperties()
        {
            if (FAIL_ADDRESS != null && FBUtilities.getBroadcastAddress().getHostAddress().equals(FAIL_ADDRESS))
            {
                logger.info("Failing stream task {} due to cassandra.test.stream.fail_address property.", session.planId());
                session.onError(new RuntimeException("Failing stream session due to cassandra.test.stream.fail_aways property."));
            }

            if (FREEZE_ADDRESS != null && FBUtilities.getBroadcastAddress().getHostAddress().equals(FREEZE_ADDRESS))
            {
                logger.info("Freezing stream task {} due to cassandra.test.stream.freeze_address property.", session.planId());
                try
                {
                    Thread.sleep(Integer.MAX_VALUE);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
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
            checkTestProperties();
            try
            {
                ReadableByteChannel in = getReadChannel(socket);
                while (!isClosed())
                {
                    // receive message
                    StreamMessage message = StreamMessage.deserialize(in, protocolVersion, session);
                    logger.debug("[Stream #{}] Received {}", session.planId(), message);
                    // Might be null if there is an error during streaming (see FileMessage.deserialize). It's ok
                    // to ignore here since we'll have asked for a retry.
                    if (message != null)
                    {
                        session.messageReceived(message);
                    }
                }
            }
            catch (SocketException e)
            {
                // socket is closed
                close();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                session.onError(t);
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
                checkTestProperties();
                DataOutputStreamAndChannel out = getWriteChannel(socket);

                StreamMessage next;
                while (!isClosed())
                {
                    if ((next = messageQueue.poll(1, TimeUnit.SECONDS)) != null)
                    {
                        logger.debug("[Stream #{}] Sending {}", session.planId(), next);
                        sendMessage(out, next);
                        if (next.type == StreamMessage.Type.SESSION_FAILED)
                            close();
                    }
                }

                // Sends the last messages on the queue
                while ((next = messageQueue.poll()) != null)
                    sendMessage(out, next);
            }
            catch (InterruptedException e)
            {
                throw new AssertionError(e);
            }
            catch (Throwable e)
            {
                session.onError(e);
            }
            finally
            {
                signalCloseDone();
            }
        }

        private void sendMessage(DataOutputStreamAndChannel out, StreamMessage message)
        {
            try
            {
                StreamMessage.serialize(message, out, protocolVersion, session);
            }
            catch (SocketException e)
            {
                session.onError(e);
                close();
            }
            catch (IOException e)
            {
                session.onError(e);
            }
        }
    }
}
