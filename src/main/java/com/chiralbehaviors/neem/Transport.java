/*
 * Copyright (c) 2014 Chiral Behaviors, all rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chiralbehaviors.neem;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import net.sf.neem.impl.ConnectionListener;
import net.sf.neem.impl.DataListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.hellblazer.pinkie.buffer.BufferProtocolFactory;
import com.hellblazer.pinkie.buffer.BufferProtocolHandler;

/**
 * @author hhildebrand
 * 
 */
public class Transport {
    private class ConnectionFactory extends BufferProtocolFactory {

        /* (non-Javadoc)
         * @see com.hellblazer.pinkie.buffer.BufferProtocolFactory#constructBufferProtocolHandler()
         */
        @Override
        public BufferProtocolHandler constructBufferProtocolHandler() {
            Connection connection = new Connection(Transport.this,
                                                   server.getLocalAddress());
            connections.add(connection);
            return connection;
        }

        /* (non-Javadoc)
         * @see com.hellblazer.pinkie.buffer.BufferProtocolFactory#constructBufferProtocolHandler(java.net.InetSocketAddress)
         */
        @Override
        public BufferProtocolHandler constructBufferProtocolHandler(InetSocketAddress remoteAddress) {
            Connection connection = new Connection(Transport.this,
                                                   remoteAddress);
            connections.add(connection);
            return connection;
        }
    }

    private static final Logger              logger            = LoggerFactory.getLogger(Transport.class);

    private int                              accepted;
    private int                              bufferSize        = 1024;
    private int                              bytesIn;
    private int                              bytesOut;
    private int                              connected;
    private final Set<Connection>            connections       = new CopyOnWriteArraySet<>();
    private int                              pktIn;
    private int                              pktOut;
    private int                              queueSize         = 10;
    private Random                           random;
    private final AtomicBoolean              running           = new AtomicBoolean();
    private ScheduledExecutorService         scheduler;
    private final ServerSocketChannelHandler server;
    private final Map<Short, DataListener>   handlers          = new ConcurrentHashMap<Short, DataListener>();
    private final ConnectionFactory          connectionFactory = new ConnectionFactory();

    private ConnectionListener               chandler;

    public Transport(String name, SocketOptions options,
                     InetSocketAddress endpoint) throws IOException {
        this.server = new ServerSocketChannelHandler(name, options, endpoint,
                                                     scheduler,
                                                     connectionFactory);
    }

    void deliver(final Connection source, final Short prt,
                 final ByteBuffer[] msg) {
        final DataListener handler = handlers.get(prt);
        if (handler == null) {
            return;
        }
        queue(new Runnable() {
            public void run() {
                try {
                    handler.receive(msg, source, prt);
                } catch (BufferUnderflowException e) {
                    logger.warn(String.format("corrupt or truncated message from %s ",
                                              source.getRemoteAddress()), e);
                    source.close();
                }
            }
        });
    }

    /**
     * Get local address.
     */
    public InetSocketAddress getLocalSocketAddress() {
        return server.getLocalAddress();
    }

    public void add(InetSocketAddress addr) {
        try {
            connectionFactory.connect(addr, server);
        } catch (IOException e) {
            logger.warn("failed to add peer " + addr, e);
        }
    }

    /**
     * @return the queueSize
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * @param queueSize
     *            the queueSize to set
     */
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        server.start();
    }

    public void terminate() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        server.terminate();
    }

    public void setConnectionListener(ConnectionListener handler) {
        chandler = handler;
    }

    public void setDataListener(DataListener handler, short port) {
        handlers.put(port, handler);
    }

    /**
     * Queue processing task.
     */
    public void queue(Runnable task) {
        schedule(task, 0);
    }

    public void schedule(Runnable task, long delay) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    void notifyClose(final Connection info) {
        if (!running.get()) {
            return;
        }
        connections.remove(info);
        if (chandler != null) {
            queue(new Runnable() {
                public void run() {
                    chandler.close(info);
                }
            });
        }
    }

    void notifyOpen(final Connection info) {
        if (!running.get()) {
            return;
        }
        if (chandler != null) {
            queue(new Runnable() {
                public void run() {
                    chandler.open(info);
                }
            });
        }
    }

    /**
     * @return the accepted
     */
    public int getAccepted() {
        return accepted;
    }

    /**
     * @return the bufferSize
     */
    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * @return the bytesIn
     */
    public int getBytesIn() {
        return bytesIn;
    }

    /**
     * @return the bytesOut
     */
    public int getBytesOut() {
        return bytesOut;
    }

    /**
     * @return the connected
     */
    public int getConnected() {
        return connected;
    }

    /**
     * @return the pktIn
     */
    public int getPktIn() {
        return pktIn;
    }

    /**
     * @return the pktOut
     */
    public int getPktOut() {
        return pktOut;
    }

    /**
     * @return the random
     */
    public Random getRandom() {
        return random;
    }

    /**
     * @return the running
     */
    public AtomicBoolean getRunning() {
        return running;
    }

    /**
     * @return the scheduler
     */
    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    /**
     * @return the handlers
     */
    public Map<Short, DataListener> getHandlers() {
        return handlers;
    }

    /**
     * @return the chandler
     */
    public ConnectionListener getChandler() {
        return chandler;
    }

    /**
     * 
     */
    public void resetCounters() {
        accepted = connected = pktOut = pktIn = bytesOut = bytesIn = 0;
    }

    /**
     * @param size
     */
    public void setBufferSize(int size) {
        this.bufferSize = size;
    }
}
