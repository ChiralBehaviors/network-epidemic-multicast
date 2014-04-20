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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.UUID;

/**
 * Implementation of gossip. Like bimodal, combines a forward retransmission
 * phase with a repair phase. However, the optimistic phase is also gossip
 * based. UUIDs, instead of sequence numbers are used to identify and discard
 * duplicates.
 */
public class Gossip implements DataListener {
    public int                                mcast, deliv, dataIn, dataOut,
            ackIn, ackOut, nackIn, nackOut;

    /**
     * Map of advertised messages.
     */
    private LinkedHashMap<UUID, ByteBuffer[]> cache;

    /**
     * The Transport port used by the Gossip class instances to exchange
     * messages.
     */
    private short                             dataport, ctrlport;

    /**
     * Number of peers to relay messages to.
     */
    private int                               fanout;

    /**
     * Represents the class to which messages must be delivered.
     */
    private Application                       handler;

    /**
     * Maximum number of stored ids.
     */
    private int                               maxIds = 100;

    /**
     * ConnectionListener management module.
     */
    private Overlay                           memb;

    private int                               pullPeriod;

    /**
     * Known retransmissions.
     */
    private LinkedHashMap<UUID, Known>        queued;

    /**
     * Random number generator for selecting targets.
     */
    private Random                            rand;

    private Periodic                          retransmit;

    /**
     * Configuration of retransmission policy.
     */
    private int                               ttl, pushttl, minPullSize;

    /**
     * Creates a new instance of Gossip.
     */
    public Gossip(Random rand, Transport net, Overlay memb, short dataport,
                  short ctrlport) {
        this.memb = memb;
        this.dataport = dataport;
        this.ctrlport = ctrlport;
        this.rand = rand;

        /*
         * Default configuration suitable for ~500 nodes, 99% probability of
         * delivery, 1% node failure. Use apps.jmx.MkConfig to compute values
         * for other configurations.
         */
        fanout = 11;
        ttl = 6;
        pushttl = 2;
        minPullSize = 64;
        pullPeriod = 120;

        cache = new LinkedHashMap<UUID, ByteBuffer[]>();
        queued = new LinkedHashMap<UUID, Known>();
        retransmit = new Periodic(rand, net, pullPeriod * 2) {
            public void run() {
                retransmit();
            }
        };

        net.setDataListener(this, this.dataport);
        net.setDataListener(this, this.ctrlport);
    }

    public int getFanout() {
        return fanout;
    }

    public int getMaxIds() {
        return maxIds;
    }

    public int getMinPullSize() {
        return minPullSize;
    }

    public int getPullPeriod() {
        return pullPeriod;
    }

    public int getPushttl() {
        return pushttl;
    }

    // Configuration parameters

    public int getTtl() {
        return ttl;
    }

    public void handler(Application handler) {
        this.handler = handler;
    }

    public void multicast(ByteBuffer[] msg) {
        mcast++;
        handleData(msg, UUID.randomUUID(), (byte) 0);
    }

    public void receive(ByteBuffer[] msg, Connection info, short port) {
        UUID uuid = UUIDs.readUUIDFromBuffer(msg);
        byte hops = Buffers.sliceCompact(msg, 1).get();

        if (port == dataport) {
            handleData(msg, uuid, hops);
        } else if (port == ctrlport) {
            handleControl(uuid, hops, info);
        }
    }

    public void resetCounters() {
        mcast = deliv = dataIn = dataOut = ackIn = ackOut = nackIn = nackOut = 0;
    }

    public void setFanout(int fanout) {
        this.fanout = fanout;
    }

    public void setMaxIds(int maxIds) {
        this.maxIds = maxIds;
    }

    public void setMinPullSize(int minPullSize) {
        this.minPullSize = minPullSize;
    }

    public void setPullPeriod(int pullPeriod) {
        this.pullPeriod = pullPeriod;
    }

    public void setPushttl(int pushttl) {
        this.pushttl = pushttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    private void handleControl(UUID uuid, byte hops, Connection info) {
        ByteBuffer[] copy = cache.get(uuid);
        if (hops == 0 && copy != null) {
            // It is a nack and we (still) have it.
            copy = Buffers.clone(copy);
            info.send(copy, dataport);
            nackIn++;
            dataOut++;
        } else if (hops > 0 && copy == null) {
            ackIn++;
            Known known = queued.get(uuid);
            if (known == null) {
                known = new Known(uuid, info);
                queued.put(uuid, known);
                purgeQueued();
            } else {
                known.senders.add(info);
            }

            retransmit.start();
        }
    }

    private void handleData(ByteBuffer[] msg, UUID uuid, byte hops) {
        dataIn++;

        if (cache.containsKey(uuid)) {
            return;
        }

        cache.put(uuid, null);
        queued.remove(uuid);

        ByteBuffer[] copy = Buffers.clone(msg);

        if (hops > 0) {
            handler.deliver(msg);
            deliv++;
        }

        hops++;

        if (hops > ttl) {
            return;
        }

        ByteBuffer[] out = new ByteBuffer[copy.length + 2];
        out[0] = UUIDs.writeUUIDToBuffer(uuid);
        out[1] = ByteBuffer.wrap(new byte[] { hops });
        System.arraycopy(copy, 0, out, 2, copy.length);
        short port = dataport;

        if (hops > pushttl && Buffers.count(copy) >= minPullSize) {
            // Cache message
            cache.put(uuid, out);

            // Send out advertisements
            out = new ByteBuffer[2];
            out[0] = UUIDs.writeUUIDToBuffer(uuid);
            out[1] = ByteBuffer.wrap(new byte[] { hops });
            port = ctrlport;

            ackOut += fanout;
        } else {
            dataOut += fanout;
        }

        relay(out, fanout, port, memb.connections());
        purgeCache();
    }

    private void purgeCache() {
        Iterator<UUID> i = cache.keySet().iterator();
        while (i.hasNext() && cache.size() > maxIds) {
            i.next();
            i.remove();
        }
    }

    private void purgeQueued() {
        Iterator<UUID> i = queued.keySet().iterator();
        while (i.hasNext() && queued.size() > maxIds) {
            i.next();
            i.remove();
        }
    }

    private void relay(ByteBuffer[] msg, int fanout, short syncport,
                       Connection[] conns) {
        // Select destinations
        int[] universe = RandomSamples.mkUniverse(conns.length);
        int samples = RandomSamples.uniformSample(fanout, universe, rand);

        // Forward
        for (int i = 0; i < samples; i++) {
            conns[universe[i]].send(Buffers.clone(msg), syncport);
        }
    }

    // Statistics

    private void request(Known known, long time) {
        nackOut++;
        known.last = time;
        Connection info = known.senders.remove(known.senders.size() - 1);

        ByteBuffer uuid_bytes = UUIDs.writeUUIDToBuffer(known.id);
        ByteBuffer[] out = new ByteBuffer[2];

        out[0] = uuid_bytes;
        out[1] = ByteBuffer.wrap(new byte[] { 0 });
        info.send(out, ctrlport);
    }

    private void retransmit() {
        Iterator<Known> i = queued.values().iterator();
        long time = System.nanoTime();
        while (i.hasNext()) {
            Known known = i.next();
            if (time - known.last < pullPeriod * 1000000L) {
                continue;
            }
            if (known.senders.isEmpty()) {
                i.remove();
            } else {
                request(known, time);
            }
        }
        if (queued.isEmpty()) {
            retransmit.stop();
        }
    }
}
