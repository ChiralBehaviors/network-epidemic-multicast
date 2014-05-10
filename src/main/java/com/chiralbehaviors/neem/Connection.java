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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.UUID;

import com.hellblazer.pinkie.buffer.BufferProtocol;
import com.hellblazer.pinkie.buffer.BufferProtocolHandler;

/**
 * @author hhildebrand
 * 
 */
public class Connection implements BufferProtocolHandler {
    private UUID              id;
    private BufferProtocol    protocol;
    private final Transport   transport;
    private InetSocketAddress listen;

    public void close() {
        protocol.close();
    }

    /**
     * @return the listen
     */
    public InetSocketAddress getListen() {
        return listen;
    }

    public Connection(Transport transport, InetSocketAddress listen) {
        this.transport = transport;
        this.listen = listen;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#accepted(com.hellblazer.pinkie.buffer.BufferProtocol)
     */
    @Override
    public void accepted(BufferProtocol protocol) {
        this.protocol = protocol;
        transport.notifyOpen(this);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#closing()
     */
    @Override
    public void closing() {
        transport.notifyClose(this);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#connected(com.hellblazer.pinkie.buffer.BufferProtocol)
     */
    @Override
    public void connected(BufferProtocol protocol) {
        this.protocol = protocol;
        transport.notifyOpen(this);
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#newReadBuffer()
     */
    @Override
    public ByteBuffer newReadBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#newWriteBuffer()
     */
    @Override
    public ByteBuffer newWriteBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#readError()
     */
    @Override
    public void readError() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#readReady()
     */
    @Override
    public void readReady() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#writeError()
     */
    @Override
    public void writeError() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see com.hellblazer.pinkie.buffer.BufferProtocolHandler#writeReady()
     */
    @Override
    public void writeReady() {
        // TODO Auto-generated method stub

    }

    /**
     * @return
     */
    public UUID getId() {
        return id;
    }

    /**
     * @param byteBuffers
     * @param idport
     */
    public void send(ByteBuffer[] byteBuffers, short idport) {
        // TODO Auto-generated method stub

    }

    public void setId(UUID id) {
        this.id = id;
    }

    /**
     * @return
     */
    public InetSocketAddress getRemoteAddress() {
        return (InetSocketAddress) protocol.getRemoteAddress();
    }

    /**
     * @param addr
     */
    public void setListen(InetSocketAddress addr) {
        this.listen = addr;
    }
}
