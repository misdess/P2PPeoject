package se.kth.swim;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kth.swim.msg.Ping;
import se.kth.swim.msg.PingReq;
import se.kth.swim.msg.Pong;
import se.kth.swim.msg.Status;
import se.kth.swim.msg.net.NetDeadParent;
import se.kth.swim.msg.net.NetPing;
import se.kth.swim.msg.net.NetPingReq;
import se.kth.swim.msg.net.NetPong;
import se.kth.swim.msg.net.NetStatus;
import se.kth.swim.simulation.CircularFifoQueue;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;

public class SwimComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(SwimComp.class);
    private Positive<Network> network = requires(Network.class);
    private final Positive<Timer> timer = requires(Timer.class);

    private final NatedAddress selfAddress;
    private final Set<NatedAddress> bootstrapNodes;
    private final NatedAddress aggregatorAddress;
    private Queue<Pair<NatedAddress, Integer>> alive;
    private Queue<Pair<NatedAddress, Integer>> failed;
    private Queue<Pair<NatedAddress, Integer>> suspected;

    private final Set<Pair<NatedAddress, Integer>> neighborSet;
    private Set<Pair<Pair<NatedAddress, Integer>, UUID>> pongTimeouts;
    private Set<Pair<Pair<NatedAddress, Integer>, UUID>> suspectTimeouts;
    private Set<Pair<NatedAddress, NatedAddress>> kPingMid;
    private final int memberSize;//size of the lists: alive, failed, suspected
    private final int messageSize;//  size of the updates.
    private int incar;
    private int k;//k-indirect ping size
    private UUID pongTimeoutId;
    private UUID suspectTimeoutId;
    private UUID pingTimeoutId;
    private UUID statusTimeoutId;
    private int receivedPings = 0;

    public SwimComp(SwimInit init) {
        this.selfAddress = init.selfAddress;
       // log.info("{} initiating...", selfAddress);
        this.bootstrapNodes = init.bootstrapNodes;
        this.aggregatorAddress = init.aggregatorAddress;

        k = 3;
        incar = 0;
        memberSize = 20;
        messageSize=4;//the middle half of the members are piggybacked
        alive = new CircularFifoQueue<Pair<NatedAddress, Integer>>(memberSize);
        failed = new CircularFifoQueue<Pair<NatedAddress, Integer>>(memberSize);
        suspected = new CircularFifoQueue<Pair<NatedAddress, Integer>>(memberSize);
        kPingMid = new HashSet<Pair<NatedAddress, NatedAddress>>();
        neighborSet = new HashSet<Pair<NatedAddress, Integer>>();
        pongTimeouts = new HashSet<Pair<Pair<NatedAddress, Integer>, UUID>>();
        suspectTimeouts = new HashSet<Pair<Pair<NatedAddress, Integer>, UUID>>();

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handlePing, network);
        subscribe(handlePingTimeout, timer);
        subscribe(handleStatusTimeout, timer);
        subscribe(handlePong, network);
        subscribe(handlePongTimeout, timer);
        subscribe(handlePingReq, network);
        subscribe(handleSuspectedTimeout, timer);
        subscribe(deadParentHandler, network);
    }

    private final Handler<Start> handleStart = new Handler<Start>() {
        @Override
        public void handle(Start event) {

         //   log.info("{} starting...", new Object[]{selfAddress.getId()});
            if (!bootstrapNodes.isEmpty()) {
                Iterator<NatedAddress> it = bootstrapNodes.iterator();
                while (it.hasNext()) {
                    NatedAddress next = it.next();
                    if (next != selfAddress) {
                        neighborSet.add(Pair.with(next, 0));
                        alive.add(Pair.with(next, 0));
                    }
                }
                schedulePeriodicPing();
            }
            schedulePeriodicStatus();
        }
    };

    private final Handler<Stop> handleStop = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            //log.info("{} stopping...", new Object[]{selfAddress.getId()});
            if (pingTimeoutId != null) {
                cancelPeriodicPing();
            }
            if (statusTimeoutId != null) {
                cancelPeriodicStatus();
            }
        }
    };

    private final Handler<PingTimeout> handlePingTimeout = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {
            Pair<NatedAddress, Integer> partnerAddress = randomNode();
            //  log.info("{}-------------- sending ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
            trigger(new NetPing(selfAddress, partnerAddress.getValue0(), new Ping(incar)), network);
            startPongTimeout(partnerAddress);
        }
    };

    private final Handler<NetPing> handlePing = new Handler<NetPing>() {

        @Override
        public void handle(NetPing event) {
            //log.info("{} received ping from:{}", new Object[]{selfAddress.getId(), event.getHeader().getSource()});
            receivedPings++;

            trigger(new NetPong(selfAddress, event.getSource(), new Pong(aliveSubset(), failed, suspected, incar)), network);
            gossipUpdates(event);
            if (selfAddress != event.getSource()) {
                neighborSet.add(Pair.with(event.getSource(), event.getContent().getIncarnation()));
                alive.add(Pair.with(event.getSource(), event.getContent().getIncarnation()));
            }
        }
    };

    private final Handler<StatusTimeout> handleStatusTimeout = new Handler<StatusTimeout>() {

        @Override
        public void handle(StatusTimeout event) {
            //log.info("{} sending status to aggregator:{}", new Object[]{selfAddress.getId(), aggregatorAddress});
            if (!failed.isEmpty()) {
                trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings, failed.peek().getValue0())), network);
            } else {
                trigger(new NetStatus(selfAddress, aggregatorAddress, new Status(receivedPings)), network);
            }
        }
    };

    public static class SwimInit extends Init<SwimComp> {

        public final NatedAddress selfAddress;
        public final Set<NatedAddress> bootstrapNodes;
        public final NatedAddress aggregatorAddress;

        public SwimInit(NatedAddress selfAddress, Set<NatedAddress> bootstrapNodes, NatedAddress aggregatorAddress) {
            this.selfAddress = selfAddress;
            this.bootstrapNodes = bootstrapNodes;
            this.aggregatorAddress = aggregatorAddress;
        }
    }

    private static class StatusTimeout extends Timeout {

        public StatusTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private void schedulePeriodicStatus() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(100, 100);
        StatusTimeout sc = new StatusTimeout(spt);
        spt.setTimeoutEvent(sc);
        statusTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicStatus() {
        CancelTimeout cpt = new CancelTimeout(statusTimeoutId);
        trigger(cpt, timer);
        statusTimeoutId = null;
    }

    private void schedulePeriodicPing() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(1000, 1000);// (delay period)
        PingTimeout sc = new PingTimeout(spt);
        spt.setTimeoutEvent(sc);
        pingTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private void cancelPeriodicPing() {
        CancelTimeout cpt = new CancelTimeout(pingTimeoutId);
        trigger(cpt, timer);
        pingTimeoutId = null;
        pongTimeoutId = null;
        suspectTimeoutId = null;

        for (Pair<Pair<NatedAddress, Integer>, UUID> me : pongTimeouts) {
            cpt = new CancelTimeout(me.getValue1());
            trigger(cpt, timer);
        }
    }

    private static class PingTimeout extends Timeout {

        public PingTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private final Handler<NetPong> handlePong = new Handler<NetPong>() {
        @Override
        public void handle(NetPong event) {
            //log.info("{} received pong from:{}", new Object[]{selfAddress.getId(), event.getHeader().getSource()});

            NatedAddress pinged = event.getSource();
            cancelPongTimeout(pinged);

            Queue<Pair<NatedAddress, Integer>> it = event.getContent().getNew();
            Iterator<Pair<NatedAddress, Integer>> iterator = it.iterator();
            while (iterator.hasNext()) {
                Pair<NatedAddress, Integer> msg = iterator.next();
                orderAlive(msg);
            }
            if (event.getSource() != selfAddress) {
                alive.add(Pair.with(event.getSource(), event.getContent().getIncar()));//add the source node to alive list
            }
            iterator = event.getContent().getFailed().iterator();
            while (iterator.hasNext()) {
                Pair<NatedAddress, Integer> msg = iterator.next();
                orderConfirmed(msg);
            }
            iterator = event.getContent().getSuspected().iterator();
            while (iterator.hasNext()) {
                Pair<NatedAddress, Integer> sus = iterator.next();
                if (sus.getValue0() == selfAddress) {
                    incar++;
                    // send alive message inside ping
                    Ping ping = new Ping(Pair.with(selfAddress, incar), null, null, incar);
                    for (Pair<NatedAddress, Integer> partnerAddress : alive) {
                        if (partnerAddress.getValue0() != selfAddress) {
                            //log.info("{} sending alive ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
                            trigger(new NetPing(selfAddress, partnerAddress.getValue0(), ping), network);
                            startPongTimeout(partnerAddress);
                        }
                    }
                }
            }

            if (kPingMid != null) {
                for (Iterator<Pair<NatedAddress, NatedAddress>> i = kPingMid.iterator(); i.hasNext();) {
                    Pair<NatedAddress, NatedAddress> p = i.next();
                    if (pinged == p.getValue1()) {// relay the pong to the source of the suspect
                        trigger(new NetPong(selfAddress, p.getValue0(), new Pong(aliveSubset(), failed, suspected, incar)), network);
                        i.remove();
                    }
                }
            }
        }
    };

    private final Handler<PongTimeout> handlePongTimeout = new Handler<PongTimeout>() {

        @Override
        public void handle(PongTimeout event) {
            Pair<NatedAddress, Integer> pinged = null;
            for (Pair<Pair<NatedAddress, Integer>, UUID> timeouts : pongTimeouts) {
                if (event.getTimeoutId().equals(timeouts.getValue1())) {
                    pongTimeouts.remove(timeouts);
                    pinged = timeouts.getValue0();
                    break;
                }
            }
            startSuspectTimeout(pinged);

            suspected.add(pinged);

            //gossip suspect message inside ping
            List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
            open.addAll(alive);
            int i = 0;
//            for (Pair<NatedAddress, Integer> partnerAddress : open) {
//                if (partnerAddress.getValue0() != selfAddress) {
//                    log.info("{} sending suspect ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
//                    Ping ping = new Ping(null, partnerAddress, null, incar);
//
//                    trigger(new NetPing(selfAddress, partnerAddress.getValue0(), ping), network);
//                    startPongTimeout(partnerAddress);
//                    if(i++>4)
//                        break;
//                }
//            }
            i = 0;
            boolean found = true;
            while (i < k) {
                Pair<NatedAddress, Integer> partnerAddress = randomNode();
                if (!pinged.getValue0().getId().equals(partnerAddress.getValue0().getId())) {
                    trigger(new NetPingReq(selfAddress, partnerAddress.getValue0(), new PingReq(pinged)), network);
                    i++;
                    found = false;
                }
                if (found) {
                    i++;
                    found = true;
                }
            }
        }
    };

    private final Handler<NetPingReq> handlePingReq = new Handler<NetPingReq>() {

        @Override
        public void handle(NetPingReq event) {
            Pair<NatedAddress, Integer> target = event.getContent().getSuspected();
            trigger(new NetPing(selfAddress, target.getValue0(), new Ping(incar)), network);
            startPongTimeout(target);
            kPingMid.add(Pair.with(selfAddress, target.getValue0()));
        }
    };

    private void startPongTimeout(Pair<NatedAddress, Integer> address) {
        ScheduleTimeout st = new ScheduleTimeout(500);// wait for 400 miliseconds
        PongTimeout pt = new PongTimeout(st);
        st.setTimeoutEvent(pt);
        pongTimeoutId = pt.getTimeoutId();
        pongTimeouts.add(Pair.with(address, pongTimeoutId));
        trigger(st, timer);
    }

    private static class PongTimeout extends Timeout {

        public PongTimeout(ScheduleTimeout request) {
            super(request);
        }
    }

    private void cancelPongTimeout(NatedAddress address) {
        for (Pair<Pair<NatedAddress, Integer>, UUID> timeouts : pongTimeouts) {
            if (address.equals(timeouts.getValue0().getValue0())) {
                pongTimeoutId = timeouts.getValue1();
                CancelTimeout cpt = new CancelTimeout(pongTimeoutId);
                pongTimeouts.remove(timeouts);
                trigger(cpt, timer);
                pongTimeoutId = null;
                break;
            }
        }
    }

    private final Handler<SuspectTimeout> handleSuspectedTimeout = new Handler<SuspectTimeout>() {
        // add the node to failed list
        @Override
        public void handle(SuspectTimeout event) {

            Pair<NatedAddress, Integer> pinged = null;
            for (Pair<Pair<NatedAddress, Integer>, UUID> timeouts : suspectTimeouts) {
                if (event.getTimeoutId().equals(timeouts.getValue1())) {
                    suspectTimeouts.remove(timeouts);
                    pinged = timeouts.getValue0();
                    break;
                }
            }
            failed.add(pinged);
            suspected.remove(pinged);
            alive.remove(pinged);
            // gossip confirm message inside ping
            List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
            open.addAll(alive);
            for (Pair<NatedAddress, Integer> partnerAddress : open) {
                if (partnerAddress.getValue0() != selfAddress) {
                    log.info("{} sending confirm ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
                    Ping ping = new Ping(null, null, pinged, incar);
                    trigger(new NetPing(selfAddress, partnerAddress.getValue0(), ping), network);
                    startPongTimeout(partnerAddress);
                }
            }
        }
    };

    private void startSuspectTimeout(Pair<NatedAddress, Integer> target) {

        for (Pair<NatedAddress, Integer> timeouts : suspected) {
            if (timeouts.getValue0().equals(target.getValue0())) {
                return;//the target node is already suspected by this node
            }
        }

        ScheduleTimeout st = new ScheduleTimeout(2000);// wait for 2700 miliseconds
        SuspectTimeout pt = new SuspectTimeout(st);
        st.setTimeoutEvent(pt);
        suspectTimeoutId = pt.getTimeoutId();
        suspectTimeouts.add(Pair.with(target, suspectTimeoutId));
        trigger(st, timer);
    }

    private static class SuspectTimeout extends Timeout {

        public SuspectTimeout(ScheduleTimeout request) {
            super(request);
        }
    }

    private void cancelSuspectTimeout(Pair<NatedAddress, Integer> address) {

        for (Pair<Pair<NatedAddress, Integer>, UUID> timeouts : suspectTimeouts) {
            if (address.getValue0().equals(timeouts.getValue0().getValue0()) && address.getValue1() >= timeouts.getValue0().getValue1()) {
                suspectTimeoutId = timeouts.getValue1();
                CancelTimeout cpt = new CancelTimeout(suspectTimeoutId);
                suspectTimeouts.remove(timeouts);
                trigger(cpt, timer);
                suspectTimeoutId = null;
                break;
            }
        }
    }

    private void gossipUpdates(NetPing event) {

        Pair<NatedAddress, Integer> msg = event.getContent().getSuspect();
        if (msg != null) {
            orderSuspect(msg);//make message ordering for suspect here
        }
        msg = event.getContent().getAlive();
        if (msg != null) {
            orderAlive(msg);//make message ordering for alive here
        }
        msg = event.getContent().getConfirm();
        if (msg != null) {//make message ordering for confirm here
            orderConfirmed(msg);
        }
    }

    private Pair<NatedAddress, Integer> randomNode() {

        List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
        int i = 0;
        for (Pair<NatedAddress, Integer> partnerAddress : neighborSet) {
            open.add(partnerAddress);
            i++;
        }
        for (Pair<NatedAddress, Integer> partnerAddress : alive) {
            open.add(partnerAddress);
            i++;
        }
        Random rand = new Random();
        return open.get(rand.nextInt(i));
    }

    private void orderAlive(Pair<NatedAddress, Integer> msg) {

        for (Pair<NatedAddress, Integer> part : suspected) {
            if (part != null && part.getValue0().equals(msg.getValue0()) && part.getValue1() < msg.getValue1()) {
                suspected.remove(part);
                cancelSuspectTimeout(part);
            }
        }
        for (Pair<NatedAddress, Integer> part : alive) {
            if (part.getValue0().equals(msg.getValue0())) {
                if (part.getValue0() != selfAddress && part.getValue1() < msg.getValue1()) {
                    alive.remove(part);
                    alive.add(msg);
                }
            } else {
                if (part.getValue0() != selfAddress) {
                    alive.add(msg);
                }
            }
        }
    }

    private void orderConfirmed(Pair<NatedAddress, Integer> msg) {

        for (Pair<NatedAddress, Integer> part : suspected) {
            if (part != null && part.getValue0().equals(msg.getValue0()) && part.getValue1() < msg.getValue1()) {
                suspected.remove(part);
                cancelSuspectTimeout(part);
            }
        }
        for (Pair<NatedAddress, Integer> part : alive) {
            if (part != null && part.getValue0().equals(msg.getValue0()) && part.getValue1() < msg.getValue1()) {
                alive.remove(part);
            }
        }
        for (Pair<NatedAddress, Integer> part : failed) {
            if (part.getValue0().equals(msg.getValue0())) {
                if (part.getValue1() < msg.getValue1()) {
                    failed.remove(part);
                    failed.add(msg);
                }
            } else {
                failed.add(msg);
            }
        }
    }

    private void orderSuspect(Pair<NatedAddress, Integer> sus) {

        if (sus.getValue0() == selfAddress) {
            incar++;
            // send alive message inside ping

            Ping ping = new Ping(Pair.with(selfAddress, incar), null, null, incar);
            for (Pair<NatedAddress, Integer> partnerAddress : alive) {
                if (partnerAddress.getValue0() != selfAddress) {
                    //log.info("{} sending alive ping to partner:{}", new Object[]{selfAddress.getId(), partnerAddress});
                    trigger(new NetPing(selfAddress, partnerAddress.getValue0(), ping), network);
                    startPongTimeout(partnerAddress);
                }
            }
        } else {
            for (Pair<NatedAddress, Integer> part : alive) {
                if (part.getValue0().equals(sus.getValue0()) && part.getValue1() < sus.getValue1()) {
                    alive.remove(part);
                }
            }
            for (Pair<NatedAddress, Integer> part : suspected) {
                if (part.getValue0().equals(sus.getValue0())) {
                    if (part.getValue1() <= sus.getValue1()) {
                        suspected.remove(part);
                        cancelSuspectTimeout(part);
                        suspected.add(sus);
                        return;
                    }
                } else {
                    suspected.add(sus);
                    return;
                }
            }
        }
    }

    private final Handler<NetDeadParent> deadParentHandler = new Handler<NetDeadParent>() {

        @Override
        public void handle(NetDeadParent event) {

            NatedAddress deadParent = event.getContent().getDead();
            int i = 0;
            if (deadParent != null) {
                if (!alive.isEmpty()) {
                    for (Pair<NatedAddress, Integer> mem : alive) {
                        if (mem != null && deadParent.equals(mem.getValue0())) {
                            i = mem.getValue1();
                            alive.remove(mem);
                        }
                    }
                }
                failed.add(Pair.with(deadParent, i + 1));
            }
        }
    };

    private Queue<Pair<NatedAddress, Integer>> aliveSubset() {
        int size = alive.size() / messageSize;
        if (size * messageSize < 6) {
            return alive;
        }
        Queue<Pair<NatedAddress, Integer>> al = new CircularFifoQueue<Pair<NatedAddress, Integer>>(2 * size);
        List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
        open.addAll(alive);
        for (int i = size; i < 3 * size; i++) {
            al.add(open.get(i));
        }
        return al;
    }

    private Queue<Pair<NatedAddress, Integer>> failedSubset() {
        int size = failed.size() / messageSize;
        if (size * messageSize < 6) {
            return failed;
        }
        Queue<Pair<NatedAddress, Integer>> al = new CircularFifoQueue<Pair<NatedAddress, Integer>>(2 * size);
        List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
        open.addAll(failed);
        for (int i = size; i < 3 * size; i++) {
            al.add(open.get(i));
        }
        return al;
    }

    private Queue<Pair<NatedAddress, Integer>> suspectedSubset() {
        int size = suspected.size() / messageSize;
        if (size * messageSize < 5) {
            return suspected;
        }
        Queue<Pair<NatedAddress, Integer>> al = new CircularFifoQueue<Pair<NatedAddress, Integer>>(2 * size);
        List<Pair<NatedAddress, Integer>> open = new ArrayList<Pair<NatedAddress, Integer>>();
        open.addAll(suspected);
        for (int i = size; i < 3 * size; i++) {
            al.add(open.get(i));
        }
        return al;
    }
}