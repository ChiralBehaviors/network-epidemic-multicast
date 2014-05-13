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
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sf.neem.impl.ConnectionListener;
import net.sf.neem.impl.DataListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.CommunicationsHandlerFactory;
import com.hellblazer.pinkie.ServerSocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;

/**
 * @author hhildebrand
 * 
 */
public class Transport {
    private class ConnectionFactory implements CommunicationsHandlerFactory {

        /* (non-Javadoc)
         * @see com.hellblazer.pinkie.CommunicationsHandlerFactory#createCommunicationsHandler(java.nio.channels.SocketChannel)
         */
        @Override
        public CommunicationsHandler createCommunicationsHandler(SocketChannel channel) {

            Connection connection = new Connection(Transport.this,
                                                   server.getLocalAddress(),
                                                   random, queueSize);
            connections.add(connection);
            return connection;
        }
    }

    private static final Logger              logger            = LoggerFactory.getLogger(Transport.class);

    private int                              accepted;
    private int                              bufferSize        = 1024;
    private int                              bytesIn           = 0;
    private int                              bytesOut          = 0;
    private ConnectionListener               chandler;
    private int                              connected         = 0;
    private final ConnectionFactory          connectionFactory = new ConnectionFactory();
    private final Set<Connection>            connections       = new CopyOnWriteArraySet<>();
    private final Map<Short, DataListener>   handlers          = new ConcurrentHashMap<Short, DataListener>();
    private int                              pktIn             = 0;
    private int                              pktOut            = 0;
    private int                              queueSize         = 10;
    private final Random                     random;
    private final AtomicBoolean              running           = new AtomicBoolean();
    private final ScheduledExecutorService   scheduler;

    private final ServerSocketChannelHandler server;

    public Transport(String name, SocketOptions options,
                     InetSocketAddress endpoint,
                     ScheduledExecutorService scheduler, Random random)
                                                                       throws IOException {
        this.server = new ServerSocketChannelHandler(name, options, endpoint,
                                                     scheduler,
                                                     connectionFactory);
        this.scheduler = scheduler;
        this.random = random;
    }

    public void add(InetSocketAddress addr) {
        Connection connection = new Connection(Transport.this, addr, random,
                                               queueSize);
        connections.add(connection);
    }

    public int getAccepted() {
        return accepted;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getBytesIn() {
        return bytesIn;
    }

    public int getBytesOut() {
        return bytesOut;
    }

    public int getConnected() {
        return connected;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return server.getLocalAddress();
    }

    public int getPktIn() {
        return pktIn;
    }

    public int getPktOut() {
        return pktOut;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public Random getRandom() {
        return random;
    }

    public boolean getRunning() {
        return running.get();
    }

    public void incPktIn() {
        pktIn++;
    }

    public void incrementBytesIn(int i) {
        bytesIn += i;
    }

    public void queue(Runnable task) {
        schedule(task, 0);
    }

    public void resetCounters() {
        accepted = connected = pktOut = pktIn = bytesOut = bytesIn = 0;
    }

    public void schedule(Runnable task, long delay) {
        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    public void setBufferSize(int size) {
        this.bufferSize = size;
    }

    public void setConnectionListener(ConnectionListener handler) {
        chandler = handler;
    }

    public void setDataListener(DataListener handler, short port) {
        handlers.put(port, handler);
    }

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
     * @param n
     */
    public void incBytesOut(long n) {
        bytesOut += n;
    }

    /**
     * 
     */
    public void incPktOut() {
        pktOut++;
    }
}
