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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import net.sf.neem.impl.Queue;
import net.sf.neem.impl.Queued;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.utils.HexDump;
import com.hellblazer.utils.Utils;

/**
 * @author hhildebrand
 * 
 */
public class Connection implements CommunicationsHandler {
    private boolean                error             = false;
    private UUID                   id;
    private final List<ByteBuffer> incomingmb        = new ArrayList<>();
    private final ByteBuffer       inbound;
    private int                    incomingRemaining = -1;
    private InetSocketAddress      listen;
    private short                  port              = -1;
    private SocketChannelHandler   handler;
    private final Transport        transport;
    public final Queue             queue;
    public final AtomicBoolean     writing           = new AtomicBoolean();
    private final ByteBuffer       header            = ByteBuffer.allocate(6);
    private ByteBuffer[]           outgoing;
    private int                    outremaining;
    private static final Logger    log               = LoggerFactory.getLogger(Connection.class);

    public Connection(Transport transport, InetSocketAddress listen,
                      Random random, int queueSize) {
        this.transport = transport;
        this.listen = listen;
        queue = new Queue(queueSize, random);
        inbound = ByteBuffer.allocateDirect(transport.getBufferSize());
    }

    @Override
    public void accept(SocketChannelHandler handler) {
        this.handler = handler;
        transport.notifyOpen(this);
    }

    public void close() {
        handler.close();
    }

    @Override
    public void closing() {
        transport.notifyClose(this);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#connected(com.hellblazer.pinkie.buffer.BufferProtocol)
     */
    @Override
    public void connect(SocketChannelHandler handler) {
        this.handler = handler;
        transport.notifyOpen(this);
    }

    /**
     * @return
     */
    public UUID getId() {
        return id;
    }

    /**
     * @return the listen
     */
    public InetSocketAddress getListen() {
        return listen;
    }

    /**
     * @return
     */
    public InetSocketAddress getRemoteAddress() {
        try {
            return (InetSocketAddress) handler.getChannel().getRemoteAddress();
        } catch (IOException e) {
            throw new IllegalStateException("Socket has already been closed", e);
        }
    }

    @Override
    public void readReady() {
        while (true) {
            if (incomingRemaining == -1) {
                if (!readMsgSize() && !error) {
                    handler.selectForRead();
                    return;
                }
            }
            if (!readMsg() && !error) {
                handler.selectForRead();
                return;
            }
        }
    }

    /**
     * @param byteBuffers
     * @param idport
     */
    public void send(ByteBuffer[] msg, short idport) {
        Queued b = new Queued(msg, idport);
        queue.push(b);
        if (writing.compareAndSet(false, true)) {
            writeReady();
        }
    }

    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @param addr
     */
    public void setListen(InetSocketAddress addr) {
        listen = addr;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#writeReady()
     */
    @Override
    public void writeReady() {
        if (queue.isEmpty()) {
            if (writing.compareAndSet(true, false)) {
                return;
            }
        }
        while (write() && !error) {
            
        }
            ;
    }

    private boolean write() {
        try {
            if (outgoing == null) {
                Queued b = queue.pop();

                ByteBuffer[] msg = b.getMsg();

                if (msg == null) {
                    return false;
                }
                short port = b.getPort();
                int size = 0;

                for (ByteBuffer element : msg) {
                    size += element.remaining();
                }
                header.reset();
                header.putInt(size);
                header.putShort(port);
                header.flip();
                outgoing = new ByteBuffer[msg.length + 1];
                outgoing[0] = header;
                System.arraycopy(msg, 0, outgoing, 1, msg.length);
                outremaining = size + 6;
            }

            long n = handler.getChannel().write(outgoing, 0, outgoing.length);
            transport.incBytesOut(n);

            outremaining -= n;
            if (outremaining == 0) {
                transport.incPktOut();
                outgoing = null;
                return true;
            }
            return false;
        } catch (IOException e) {
            if (!Utils.isClosedConnection(e)) {
                log.warn(String.format("Error writing on %s", id), e);
            }
            error = true;
            close();
            return false;
        }
    }

    private boolean readMsg() {
        if (!read()) {
            return false;
        }
        byte[] buffer = new byte[Math.min(inbound.remaining(),
                                          incomingRemaining)];
        incomingRemaining -= buffer.length;

        transport.incrementBytesIn(buffer.length);
        inbound.get(buffer);
        incomingmb.add(ByteBuffer.wrap(buffer));
        inbound.compact();
        if (incomingRemaining == 0) {
            final ByteBuffer[] msg = incomingmb.toArray(new ByteBuffer[incomingmb.size()]);
            transport.deliver(this, port, msg);
            incomingmb.clear();
            incomingRemaining = -1;
            port = -1;
            transport.incPktIn();
            return true;
        }
        return false;
    }

    public String socketInfo() {
        SocketChannel channel = handler.getChannel();
        try {
            return String.format("[%s local=%s remote=%s]",
                                 channel.isConnected() ? "connected"
                                                      : "unconnected",
                                 channel.getLocalAddress(),
                                 channel.getRemoteAddress());
        } catch (IOException e) {
            return "[ioexception getting socket info]";
        }
    }

    private boolean readMsgSize() {
        if (!read()) {
            return false;
        }
        if (inbound.remaining() >= 6) {
            incomingRemaining = inbound.getInt();
            port = inbound.getShort();
            inbound.compact();
            transport.incrementBytesIn(6);
            return true;
        }
        return false;
    }

    private boolean read() {
        try {
            int position = inbound.position();
            int read = handler.getChannel().read(inbound);
            if (log.isDebugEnabled()) {
                log.debug(String.format("socket %s read %s bytes:\n%s",
                                        socketInfo(),
                                        read,
                                        HexDump.hexdump(inbound, position, read)));
            }
            return true;
        } catch (IOException e) {
            error = true;
            if (Utils.isClosedConnection(e)) {
                if (log.isDebugEnabled()) {
                    log.debug("socket {} closed during read", socketInfo());
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("socket {} errored during read", socketInfo(), e);
                }
            }
            handler.close();
            return false;
        }
    }
}
