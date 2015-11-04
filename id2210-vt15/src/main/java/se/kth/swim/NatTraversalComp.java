/*
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) Copyright (C)
 * 2009 Royal Institute of Technology (KTH)
 *
 * GVoD is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.kth.swim;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.croupier.CroupierPort;
import se.kth.swim.croupier.msg.CroupierSample;
import se.kth.swim.croupier.util.Container;
import se.kth.swim.msg.deadParent;
import se.kth.swim.msg.net.NetDeadParent;
import se.kth.swim.msg.net.NetMsg;
import se.kth.swim.msg.net.NetPingParent;
import se.kth.swim.msg.net.NetPongParent;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Header;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;
import se.sics.p2ptoolbox.util.network.impl.RelayHeader;
import se.sics.p2ptoolbox.util.network.impl.SourceHeader;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class NatTraversalComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(NatTraversalComp.class);
    private Negative<Network> local = provides(Network.class);
    private Positive<Network> network = requires(Network.class);
    private final Positive<CroupierPort> croupier = requires(CroupierPort.class);

    private final Positive<Timer> timer = requires(Timer.class);

    private final NatedAddress selfAddress;
    private final Random rand;

    private UUID pongTimeoutId;
    private UUID pingTimeoutId;
    private Set<Pair<NatedAddress, UUID>> pongTimeouts;

    public NatTraversalComp(NatTraversalInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} {} initiating...", new Object[]{selfAddress.getId(), (selfAddress.isOpen() ? "OPEN" : "NATED")});
        this.rand = new Random(init.seed);
        pongTimeouts = new HashSet<Pair<NatedAddress, UUID>>();

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleIncomingMsg, network);
        subscribe(handleOutgoingMsg, local);
        subscribe(handleCroupierSample, croupier);

        subscribe(handlePingTimeout, timer);
        subscribe(handlePongTimeout, timer);
        subscribe(handlePing, network);
        subscribe(handlePong, network);
    }

    private final Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress.getId()});
            schedulePeriodicPing();
        }
    };

    private final Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress.getId()});
            cancelPeriodicPing();
        }
    };

    private final Handler<NetMsg<Object>> handleIncomingMsg = new Handler<NetMsg<Object>>() {

        @Override
        public void handle(NetMsg<Object> msg) {
            //  log.trace("{} received msg:{}", new Object[]{selfAddress.getId(), msg});
            Header<NatedAddress> header = msg.getHeader();
            if (header instanceof SourceHeader) {
                if (!selfAddress.isOpen()) {
                    throw new RuntimeException("source header msg received on nated node - nat traversal logic error");
                }
                SourceHeader<NatedAddress> sourceHeader = (SourceHeader<NatedAddress>) header;
                if (sourceHeader.getActualDestination().getParents().contains(selfAddress)) {
                    // log.info("{} **********relaying message for:{}", new Object[]{selfAddress.getId(), sourceHeader.getSource()});
                    RelayHeader<NatedAddress> relayHeader = sourceHeader.getRelayHeader();
                    trigger(msg.copyMessage(relayHeader), network);
                } else {
                    //  log.warn("{} received weird relay message:{} - dropping it", new Object[]{selfAddress.getId(), msg});

                }
            } else if (header instanceof RelayHeader) {
                if (selfAddress.isOpen()) {
                    throw new RuntimeException("relay header msg received on open node - nat traversal logic error");
                }
                RelayHeader<NatedAddress> relayHeader = (RelayHeader<NatedAddress>) header;
                //  log.info("{} delivering relayed message:{} from:{}", new Object[]{selfAddress.getId(), msg, relayHeader.getActualSource()});
                Header<NatedAddress> originalHeader = relayHeader.getActualHeader();
                trigger(msg.copyMessage(originalHeader), local);
            } else {
                // log.info("{} delivering direct message:{} from:{}", new Object[]{selfAddress.getId(), msg, header.getSource()});
                trigger(msg, local);
            }
        }
    };

    private final Handler<NetMsg<Object>> handleOutgoingMsg = new Handler<NetMsg<Object>>() {

        @Override
        public void handle(NetMsg<Object> msg) {
            //log.trace("{} sending msg:{}", new Object[]{selfAddress.getId(), msg});
            Header<NatedAddress> header = msg.getHeader();
            if (header.getDestination().isOpen()) {
                //log.info("{} sending direct message:{} to:{}", new Object[]{selfAddress.getId(), msg, header.getDestination()});
                trigger(msg, network);
            } else {
                if (header.getDestination().getParents().isEmpty()) {
                    throw new RuntimeException("nated node with no parents");
                }
                NatedAddress parent = randomNode(header.getDestination().getParents());
                SourceHeader<NatedAddress> sourceHeader = new SourceHeader(header, parent);
                //log.info("{} sending message:{} to relay:{}", new Object[]{selfAddress.getId(), msg, parent});
                if (msg.copyMessage(sourceHeader) != null) {
                    //System.out.println(header.getDestination().getId()+" nated behind " + header.getDestination().getParents());
                    trigger(msg.copyMessage(sourceHeader), network);
                }
            }
        }
    };

    private final Handler handleCroupierSample = new Handler<CroupierSample>() {
        @Override
        public void handle(CroupierSample event) {
            // log.info("{}croupier public nodes:{}", selfAddress.getBaseAdr(), event.publicSample);
            Set<NatedAddress> parents = new HashSet<NatedAddress>(3);
            Iterator<Container<NatedAddress, Object>> it = event.publicSample.iterator();
            while (it.hasNext()) {
                parents.add(it.next().getSource());
            }
            if (selfAddress.getParents().size() < 3 && !selfAddress.isOpen()) {
                selfAddress.getParents().addAll(parents);
            }
        }
    };

    private NatedAddress randomNode(Set<NatedAddress> nodes) {
        int index = rand.nextInt(nodes.size());
        Iterator<NatedAddress> it = nodes.iterator();
        while (index > 0) {
            it.next();
            index--;
        }
        return it.next();
    }

    public static class NatTraversalInit extends Init<NatTraversalComp> {

        public final NatedAddress selfAddress;
        public final long seed;

        public NatTraversalInit(NatedAddress selfAddress, long seed) {
            this.selfAddress = selfAddress;
            this.seed = seed;
        }
    }

    private void schedulePeriodicPing() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(100, 1000);// (delay period)
        PingParentTimeout sc = new PingParentTimeout(spt);
        spt.setTimeoutEvent(sc);
        pingTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicPing() {
        CancelTimeout cpt = new CancelTimeout(pingTimeoutId);
        trigger(cpt, timer);
        pingTimeoutId = null;
        pongTimeoutId = null;

        for (Pair<NatedAddress, UUID> me : pongTimeouts) {
            cpt = new CancelTimeout(me.getValue1());
            trigger(cpt, timer);
        }
    }

    private static class PingParentTimeout extends Timeout {

        public PingParentTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private final Handler<PingParentTimeout> handlePingTimeout = new Handler<PingParentTimeout>() {

        @Override
        public void handle(PingParentTimeout event) {
            Iterator<NatedAddress> it = selfAddress.getParents().iterator();
            while (it.hasNext()) {
                NatedAddress partnerAddress = it.next();
                //log.info("{} sending ping to parent:{}", new Object[]{selfAddress.getId(), partnerAddress});
                trigger(new NetPingParent(selfAddress, partnerAddress), network);
                startPongTimeout(partnerAddress);
            }
        }
    };

    private void startPongTimeout(NatedAddress address) {
        ScheduleTimeout st = new ScheduleTimeout(1000);// wait for 700 miliseconds
        PongParentTimeout pt = new PongParentTimeout(st);
        st.setTimeoutEvent(pt);
        pongTimeoutId = pt.getTimeoutId();
        pongTimeouts.add(Pair.with(address, pongTimeoutId));
        trigger(st, timer);
    }

    private static class PongParentTimeout extends Timeout {

        public PongParentTimeout(ScheduleTimeout request) {
            super(request);
        }
    }

    private final Handler<NetPingParent> handlePing = new Handler<NetPingParent>() {

        @Override
        public void handle(NetPingParent event) {
            trigger(new NetPongParent(selfAddress, event.getSource()), network);
        }
    };

    private final Handler<NetPongParent> handlePong = new Handler<NetPongParent>() {

        @Override
        public void handle(NetPongParent event) {
            Iterator<Pair<NatedAddress, UUID>> pair = pongTimeouts.iterator();
            while (pair.hasNext()) {
                Pair<NatedAddress, UUID> pair1 = pair.next();
                if (event.getSource().equals(pair1.getValue0())) {
                    pair.remove();
                }
            }
        }
    };

    private final Handler<PongParentTimeout> handlePongTimeout = new Handler<PongParentTimeout>() {

        @Override
        public void handle(PongParentTimeout event) {

            Iterator<Pair<NatedAddress, UUID>> pair = pongTimeouts.iterator();
            while (pair.hasNext()) {
                Pair<NatedAddress, UUID> pair1 = pair.next();
                if (event.getTimeoutId().equals(pair1.getValue1())) {
                    //send parent died message to swimComp
                    trigger(new NetDeadParent(selfAddress, selfAddress, new deadParent(pair1.getValue0())), local);
                    selfAddress.getParents().remove(pair1.getValue0());
                    pair.remove();
                }
            }
        }
    };
}
