/*	
 * NeEM - Network-friendly Epidemic Multicast
 * Copyright (c) 2005-2007, University of Minho
 * All rights reserved.
 *
 * Contributors:
 *  - Pedro Santos <psantos@gmail.com>
 *  - Jose Orlando Pereira <jop@di.uminho.pt>
 *
 * Partially funded by FCT, project P-SON (POSC/EIA/60941/2004).
 * See http://pson.lsd.di.uminho.pt/ for more information.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *  - Redistributions of source code must retain the above copyright
 *  notice, this list of conditions and the following disclaimer.
 * 
 *  - Redistributions in binary form must reproduce the above copyright
 *  notice, this list of conditions and the following disclaimer in the
 *  documentation and/or other materials provided with the distribution.
 * 
 *  - Neither the name of the University of Minho nor the names of its
 *  contributors may be used to endorse or promote products derived from
 *  this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package net.sf.neem.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

/**
 * Listening socket accepting connections.
 */
public class Acceptor extends Handler {
    /**
     * Socket used to listen for connections
     */
    private ServerSocketChannel sock;

    /**
     * Create a new listening socket.
     * 
     * @param trans
     *            transport object
     * @param bind
     *            local address to bind to, if any
     * @throws IOException
     */
    Acceptor(Transport trans, InetSocketAddress bind) throws IOException {
        super(trans);
        sock = ServerSocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().bind(bind);

        key = sock.register(transport.selector, SelectionKey.OP_ACCEPT);
        key.attach(this);
    }

    public InetSocketAddress getLocalSocketAddress() {
        if (sock != null) {
            return (InetSocketAddress) sock.socket().getLocalSocketAddress();
        }
        return null;
    }

    /**
     * Open connection event handler. When the handler behaves as server.
     */
    @Override
    void handleAccept() throws IOException {
        SocketChannel nsock = sock.accept();

        transport.accepted++;
        transport.notifyOpen(new Connection(transport, nsock));
    }

    /**
     * Closed connection event handler. Either by overlay management or death of
     * peer.
     */
    @Override
    void handleClose() {
        if (key == null) {
            return;
        }

        try {
            key.channel().close();
            key.cancel();
            sock.close();
            key = null;
        } catch (IOException e) {
            // Don't care, we're cleaning up anyway...
            logger.warn("cleanup failed", e);
        }
    }

    @Override
    void handleConnect() {
        // will not happen
    }

    @Override
    void handleRead() {
        // will not happen
    }

    @Override
    void handleWrite() {
        // will not happen
    }
}
