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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.chiralbehaviors.neem.Connection;
import com.chiralbehaviors.neem.Transport;

/**
 * Implementation of overlay management. This class combines a number of random
 * walks upon initial join with periodic shuffling.
 */
public class Overlay implements ConnectionListener, DataListener {
    public int                                    joins, purged, shuffleIn,
            shuffleOut;

    private int                                   fanout;

    private final short                           idport;

    private final short                           joinport;

    private final UUID                            myId;

    private final Transport                       net;

    private final InetSocketAddress               netid;

    private final ConcurrentMap<UUID, Connection> peers = new ConcurrentHashMap<UUID, Connection>();

    private final Random                          rand;

    private final Periodic                        shuffle;

    private final short                           shuffleport;

    /**
     * Creates a new instance of Overlay
     */
    public Overlay(Random rand, InetSocketAddress id, UUID myId, Transport net,
                   short joinport, short idport, short shuffleport) {
        this.rand = rand;
        netid = id;
        this.net = net;
        this.idport = idport;
        this.shuffleport = shuffleport;
        this.joinport = joinport;

        /*
         * Default configuration suitable for ~500 nodes, 99% probability of
         * connectivity, 10% node failure. Use apps.jmx.MkConfig to compute
         * values for other configurations.
         */
        fanout = 15;

        this.myId = myId;
        shuffle = new Periodic(rand, net, 10000) {
            public void run() {
                shuffle();
            }
        };

        net.setDataListener(this, this.shuffleport);
        net.setDataListener(this, this.idport);
        net.setDataListener(this, this.joinport);
        net.setConnectionListener(this);
    }

    public void close(Connection info) {
        if (info.getId() != null) {
            peers.remove(info.getId());
        }
        if (peers.isEmpty()) {
            // Disconnected. Should it notify the application?
        }
    }

    /**
     * Get all connections that have been validated.
     */
    public Connection[] connections() {
        return peers.values().toArray(new Connection[peers.size()]);
    }

    public int getFanout() {
        return fanout;
    }

    /**
     * Get globally unique ID in the overlay.
     */
    public UUID getId() {
        return myId;
    }

    public InetSocketAddress getLocalSocketAddress() {
        return netid;
    }

    /**
     * Get all peer addresses.
     */
    public InetSocketAddress[] getPeerAddresses() {
        InetSocketAddress[] addrs = new InetSocketAddress[peers.size()];
        int i = 0;
        for (Connection peer : peers.values()) {
            addrs[i++] = peer.getListen();
        }
        return addrs;
    }

    /**
     * Get all connected peers.
     */
    public UUID[] getPeers() {
        UUID[] peers = new UUID[this.peers.size()];
        peers = this.peers.keySet().toArray(peers);
        return peers;
    }

    public int getShufflePeriod() {
        return shuffle.getInterval();
    }

    public void open(Connection info) {
        info.send(new ByteBuffer[] { UUIDs.writeUUIDToBuffer(myId),
                          Addresses.writeAddressToBuffer(netid) }, idport);
        purgeConnections();
    }

    public void receive(ByteBuffer[] msg, Connection info, short port) {
        if (port == idport) {
            handleId(msg, info);
        } else if (port == shuffleport) {
            handleShuffle(msg);
        } else {
            handleJoin(msg);
        }
    }

    public void resetCounters() {
        joins = purged = shuffleIn = shuffleOut = 0;
    }

    public void setFanout(int fanout) {
        this.fanout = fanout;
    }

    // Configuration parameters

    public void setShufflePeriod(int shufflePeriod) {
        shuffle.setInterval(shufflePeriod);
    }

    /**
     * Connect two other peers by informing one of the other.
     * 
     * @param target
     *            The connection peer.
     * @param arrow
     *            The accepting peer.
     */
    public void tradePeers(Connection target, Connection arrow) {
        shuffleOut++;
        target.send(new ByteBuffer[] { UUIDs.writeUUIDToBuffer(arrow.getId()),
                            Addresses.writeAddressToBuffer(arrow.getListen()) },
                    shuffleport);
    }

    private void handleId(ByteBuffer[] msg, Connection info) {
        if (peers.isEmpty()) {
            info.send(new ByteBuffer[] { UUIDs.writeUUIDToBuffer(myId),
                    Addresses.writeAddressToBuffer(netid) }, joinport);
            shuffle.start();
        }

        UUID id = UUIDs.readUUIDFromBuffer(msg);
        InetSocketAddress addr = Addresses.readAddressFromBuffer(msg);

        if (peers.containsKey(id)) {
            info.close();
            return;
        }

        synchronized (this) {
            info.setId(id);
            info.setListen(addr);
            peers.put(id, info);
        }
    }

    private void handleJoin(ByteBuffer[] msg) {
        joins++;
        ByteBuffer[] beacon = Buffers.clone(msg);

        Connection[] conns = connections();
        for (Connection conn : conns) {
            shuffleOut++;
            conn.send(Buffers.clone(beacon), shuffleport);
        }
    }

    private void handleShuffle(ByteBuffer[] msg) {
        shuffleIn++;
        ByteBuffer[] beacon = Buffers.clone(msg);

        UUID id = UUIDs.readUUIDFromBuffer(msg);
        InetSocketAddress addr = Addresses.readAddressFromBuffer(msg);

        if (peers.containsKey(id)) {
            return;
        }

        // Flip a coin...
        if (peers.size() < fanout || rand.nextFloat() > 0.5) {
            net.add(addr);
        } else {
            shuffleOut++;
            Connection[] conns = connections();
            int idx = rand.nextInt(conns.length);
            conns[idx].send(Buffers.clone(beacon), shuffleport);
        }
    }

    // Statistics

    private void purgeConnections() {
        Connection[] conns = connections();
        int nc = conns.length;

        while (peers.size() > fanout) {
            Connection info = conns[rand.nextInt(nc)];
            peers.remove(info.getId());
            info.close();
            info.setId(null);
            purged++;
        }
    }

    /**
     * Tell a neighbor, that there is a connection do the peer identified by its
     * address, wich is sent to the peers.
     */
    private void shuffle() {
        if (peers.isEmpty()) {
            shuffle.stop();
            return;
        }

        Connection[] conns = connections();
        if (conns.length < 2) {
            return;
        }
        Connection toSend = conns[rand.nextInt(conns.length)];
        Connection toReceive = conns[rand.nextInt(conns.length)];

        if (toSend.getId() == null) {
            return;
        }

        tradePeers(toReceive, toSend);
    }
}
