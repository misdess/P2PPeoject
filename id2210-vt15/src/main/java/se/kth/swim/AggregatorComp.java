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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.kth.swim.msg.net.NetStatus;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.p2ptoolbox.util.network.NatedAddress;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class AggregatorComp extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(AggregatorComp.class);
    private final Positive<Network> network = requires(Network.class);
    private final Positive<Timer> timer = requires(Timer.class);

    private final NatedAddress selfAddress;
    private UUID reportTimeoutId;
    private Map<Integer, Set<Integer>> failed;
    int i;//the highest failed detected node

    public AggregatorComp(AggregatorInit init) {
        this.selfAddress = init.selfAddress;
        log.info("{} initiating...", new Object[]{selfAddress.getId()});

        failed = new HashMap<Integer, Set<Integer>>();

        subscribe(handleStart, control);
        subscribe(handleStop, control);
        subscribe(handleStatus, network);
        subscribe(reportTimeoutHandler, timer);
    }

    private final Handler<Start> handleStart = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("{} starting...", new Object[]{selfAddress});
            schedulePeriodicReport();
        }
    };

    private final Handler<Stop> handleStop = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            log.info("{} stopping...", new Object[]{selfAddress});
            if (reportTimeoutId != null) {
                cancelPeriodicReport();
            }
        }
    };

    private final Handler<NetStatus> handleStatus = new Handler<NetStatus>() {

        @Override
        public void handle(NetStatus status) {
//            log.info("{} status from:{} pings:{}",  new Object[]{selfAddress.getId(), status.getHeader().getSource(), status.getContent().receivedPings});
            NatedAddress m = status.getContent().getFailed();
            Set<Integer> deadNode = new HashSet<Integer>();
            if (m != null) {
                int confirmed = m.getId();
                if (failed.containsKey(confirmed)) {
                    deadNode = failed.get(confirmed);
                }
                if (deadNode.contains(status.getSource())) {
                    return;
                }
                deadNode.add(status.getSource().getId());
                failed.put(confirmed, deadNode);
            }

            if (status.getContent().getFailed1() != null) {
                for (Pair<NatedAddress, Integer> dead : status.getContent().getFailed1()) {
                    if (dead.getValue0() != null) {
                        int confirmed = dead.getValue0().getId();
                        if (failed.containsKey(confirmed)) {
                            deadNode = failed.get(confirmed);
                        }
                        if (deadNode.contains(status.getSource())) {
                            return;
                        }
                        deadNode.add(status.getSource().getId());
                        failed.put(confirmed, deadNode);
                    }
                }
            }
        }
    };

    public static class AggregatorInit extends Init<AggregatorComp> {

        public final NatedAddress selfAddress;

        public AggregatorInit(NatedAddress selfAddress) {
            this.selfAddress = selfAddress;
        }
    }

    private void schedulePeriodicReport() {
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(100, 100);// (delay period)
        ReportTimeout sc = new ReportTimeout(spt);
        spt.setTimeoutEvent(sc);
        reportTimeoutId = sc.getTimeoutId();
        trigger(spt, timer);
    }

    private static class ReportTimeout extends Timeout {

        public ReportTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    private final Handler<ReportTimeout> reportTimeoutHandler = new Handler<ReportTimeout>() {

        @Override
        public void handle(ReportTimeout event) {
            int j = 0;
            int id = 0;
            Set<Integer> max = null;
            for (Map.Entry<Integer, Set<Integer>> entry : failed.entrySet()) {
                if (max == null) {
                    max = entry.getValue();
                    id = entry.getKey();
                    continue;
                }
                if (max.size() < entry.getValue().size()) {
                    max = entry.getValue();
                    id = entry.getKey();
                }
            }
            if (max != null && i < max.size()) {
                i = max.size();
                log.info("Failed: {},  Count: {}, List: {}", new Object[]{id, max.size(), max});
            }
        }

    };

    private void cancelPeriodicReport() {
        CancelTimeout cpt = new CancelTimeout(reportTimeoutId);
        trigger(cpt, timer);
        reportTimeoutId = null;
    }
}