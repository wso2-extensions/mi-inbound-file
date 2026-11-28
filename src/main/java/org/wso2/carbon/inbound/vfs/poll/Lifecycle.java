/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.inbound.vfs.poll;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.log.UriMask;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.LongFunction;

public class Lifecycle implements EndpointHealth.RuntimeMetrics {

    public enum State { RUNNING, DRAINING, STOPPED }

    static final class Drain {
        final Generation generation;
        final long start = System.nanoTime();
        final long deadline;
        final List<VfsSession> closeOwed = new ArrayList<>();
        int resumesWaiting;
        boolean done;

        Drain(Generation generation, long timeoutMillis) {
            this.generation = generation;
            deadline = start + TimeUnit.MILLISECONDS.toNanos(Math.max(0, timeoutMillis));
        }
    }

    private static final Log LOG = LogFactory.getLog(Lifecycle.class);
    private final String name;
    private final String prefix;
    private final EndpointHealth health;
    private final EndpointHealth.RuntimeMetrics observer;
    private final LongFunction<List<VfsSession>> sessions;
    private final LongSupplier timeoutMillis;
    private final int workerCount;
    private final LongFunction<VfsSession> workerFactory;
    private final Set<Generation> late = new LinkedHashSet<>();
    private volatile State state = State.RUNNING;
    private volatile Generation current;
    private Drain drain;

    public Lifecycle(String name, EndpointHealth health, LongFunction<List<VfsSession>> sessions,
                     LongSupplier timeoutMillis) {
        this(name, health, sessions, timeoutMillis, 0, null);
    }

    public Lifecycle(String name, EndpointHealth health, LongFunction<List<VfsSession>> sessions,
                     LongSupplier timeoutMillis, int workerCount, LongFunction<VfsSession> workerFactory) {
        this.workerCount = workerCount;
        this.workerFactory = workerFactory;
        this.name = name;
        this.prefix = "[inbound:" + UriMask.mask(name).replaceAll("\\R", " ") + "] ";
        this.health = health;
        this.sessions = sessions;
        this.timeoutMillis = timeoutMillis;
        current = build(1);
        synchronized (health) {
            observer = health.observeInboundSessions(this);
            health.paused();
        }
        if (TestPoints.enabled()) {
            WeakReference<Lifecycle> reference = new WeakReference<>(this);
            TestPoints.snapshot("lifecycle", name, () -> snapshot(reference));
        }
    }

    @Override
    public int getOpenClientCount() {
        Generation generation = current;
        if (generation.retired) {
            return 0;
        }
        int total = 0;
        for (VfsSession session : generation.sessions) {
            total += session.getOpenClientCount();
        }
        return current == generation && !generation.retired ? total : 0;
    }

    private Generation build(long number) {
        List<VfsSession> all = new ArrayList<>(sessions.apply(number));
        try {
            for (int slot = 0; slot < workerCount; slot++) {
                all.add(workerFactory.apply(number));
            }
            return new Generation(number, all, this, health, name, workerCount, workerFactory);
        } catch (RuntimeException | Error failure) {
            for (VfsSession session : all) {
                session.closeAfterAbort();
            }
            throw failure;
        }
    }

    public State state() {
        return state;
    }

    private static String snapshot(WeakReference<Lifecycle> reference) {
        Lifecycle instance = reference.get();
        return instance == null ? "" : instance.snapshot();
    }

    synchronized String snapshot() {
        StringBuilder result = new StringBuilder("state=").append(state)
                .append("\tgeneration=").append(current.number).append("\tdone=").append(drain != null && drain.done);
        appendCounters(result, current);
        for (Generation generation : late) {
            if (generation != current) {
                appendCounters(result, generation);
            }
        }
        return result.toString();
    }

    private static void appendCounters(StringBuilder result, Generation generation) {
        result.append("\tg=").append(generation.number).append("\tpollActive=").append(generation.pollActive)
                .append("\tinFlight=").append(generation.inFlight);
    }

    // True when the lifecycle is stopped and nothing of the drained generation is still out. A task of an older
    // generation was retired before, and a retired task returns before every call to the fail state store, so then
    // no task can hand the store a new write any more.
    public synchronized boolean stoppedWithNoWork() {
        return state == State.STOPPED && (drain == null || !drain.generation.hasWork());
    }

    // Runs the action only when the lifecycle is stopped with no work out, under the lock that a resume holds while it
    // builds its generation and looks up the fail state store. So no resume comes between the check and the action.
    public synchronized void whenStoppedWithNoWork(Runnable action) {
        if (stoppedWithNoWork()) {
            action.run();
        }
    }

    public synchronized Generation enterPoll() {
        if (state == State.STOPPED) {
            health.namedLog().warn("The endpoint was polled while it is stopped.");
        }
        if (state != State.RUNNING || current.pollActive) {
            return null;
        }
        current.pollSession.enter();
        current.inside.add(current.pollSession);
        current.pollActive = true;
        return current;
    }

    public void exitPoll(Generation generation) {
        boolean close;
        synchronized (this) {
            close = leave(generation, generation.pollSession);
            generation.pollActive = false;
            finished(generation);
        }
        if (close) {
            generation.pollSession.closeAfterAbort();
        }
    }

    public synchronized boolean fileStarted(Generation generation) {
        if (state != State.RUNNING || generation != current) {
            return false;
        }
        generation.inFlight++;
        return true;
    }

    public synchronized boolean handOver(Generation generation, VfsSession worker, Thread thread) {
        if (generation.retired) {
            return false;
        }
        if (worker == generation.pollSession || !generation.sessions.contains(worker)
                || !generation.inside.add(worker)) {
            throw new IllegalStateException("Invalid worker session.");
        }
        worker.enter(thread);
        return true;
    }

    public void fileEnded(Generation generation, VfsSession worker) {
        workerEnded(generation, worker, false);
    }

    synchronized boolean registerWorker(Generation generation, VfsSession replacement) {
        if (state != State.RUNNING || current != generation) {
            return false;
        }
        generation.sessions.add(replacement);
        return true;
    }

    void workerEnded(Generation generation, VfsSession worker, boolean failed) {
        boolean close;
        synchronized (this) {
            if (generation.inFlight <= 0) {
                throw new IllegalStateException("No file in flight.");
            }
            close = worker != null && leave(generation, worker);
            if (failed && !generation.retired) {
                generation.sessions.remove(worker);
                close = true;
            }
            generation.inFlight--;
            finished(generation);
        }
        if (close) {
            worker.closeAfterAbort();
        }
    }

    private boolean leave(Generation generation, VfsSession session) {
        session.leave();
        if (!generation.inside.remove(session)) {
            throw new IllegalStateException("Session already released.");
        }
        if (generation.retired) {
            health.lateThread();
        }
        return generation.retired;
    }

    private void finished(Generation generation) {
        if (generation.retired && !generation.hasWork()) {
            late.remove(generation);
        }
        notifyAll();
    }

    public void stop(String hook) {
        Drain record;
        synchronized (this) {
            while (state == State.DRAINING) {
                Drain active = drain;
                if (active.resumesWaiting == 0) {
                    await(() -> active.done);
                    return;
                }
                await(() -> active.done && active.resumesWaiting == 0);
            }
            if (state == State.STOPPED) {
                return;
            }
            record = new Drain(current, timeoutMillis.getAsLong());
            drain = record;
            state = State.DRAINING;
            notifyAll();
        }
        if (record.generation.workerPool != null) {
            record.generation.workerPool.stopAdmissions();
        }
        drain(record, hook);
    }

    private void drain(Drain record, String hook) {
        boolean cleaned = false;
        try {
            point("stop.inside-bound", record);
            synchronized (this) {
                while (record.generation.hasWork()) {
                    long left = record.deadline - System.nanoTime();
                    if (left <= 0) {
                        break;
                    }
                    try {
                        TimeUnit.NANOSECONDS.timedWait(this, left);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                retire(record);
            }
            point("stop.after-retire", record);
            abortAll(record);
            closeOwed(record);
            cleaned = true;
        } catch (RuntimeException | Error failure) {
            health.namedLog().debug("Drain cleanup interrupted. " + failure.getClass().getSimpleName());
        } finally {
            try {
                if (!cleaned) {
                    synchronized (this) {
                        retire(record);
                    }
                    abortAll(record);
                }
                point("stop.before-final-step", record);
            } finally {
                synchronized (this) {
                    try {
                        synchronized (health) {
                            if (health.observesRuntime(observer)) {
                                health.paused();
                            }
                        }
                    } finally {
                        state = State.STOPPED;
                        record.done = true;
                        notifyAll();
                        // A joining hook observes the owner's completion line before it returns.
                        LOG.info(prefix + hook + " completed in "
                                + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - record.start) + " ms with "
                                + record.generation.inFlight + " files left in flight.");
                    }
                }
            }
        }
    }

    private void retire(Drain record) {
        Generation generation = record.generation;
        if (!generation.retired) {
            generation.retired = true;
            for (VfsSession session : generation.sessions) {
                if (!generation.inside.contains(session)) {
                    record.closeOwed.add(session);
                }
            }
            if (generation.hasWork() && TestPoints.enabled()) {
                late.add(generation);
            }
            notifyAll();
        }
    }

    void point(String point, Drain record) {
        if (TestPoints.enabled()) {
            String generation = "generation=" + record.generation.number;
            String endpoint = "endpoint=" + name;
            TestPoints.hold(point, name, endpoint, generation);
            if ("stop.after-retire".equals(point)) {
                TestPoints.Fault fault = TestPoints.fault(point, name, endpoint, generation);
                if (fault != null && "throw".equals(fault.token())) {
                    throw new IllegalStateException("Drain fault.");
                }
                if (fault != null) {
                    TestPoints.event("unknown-token", name, endpoint, generation, "token=" + fault.token());
                }
            }
        }
    }

    private void abortAll(Drain record) {
        if (record.generation.workerPool != null) {
            record.generation.workerPool.retire();
        }
        if (TestPoints.enabled()) {
            TestPoints.event("drain.abort.start", name, "endpoint=" + name, "generation=" + record.generation.number);
        }
        for (VfsSession session : record.generation.sessions) {
            try {
                session.abort();
            } catch (RuntimeException | Error failure) {
                health.namedLog().debug("Could not abort a session. " + failure.getClass().getSimpleName());
            }
        }
        if (TestPoints.enabled()) {
            TestPoints.event("drain.abort.end", name, "endpoint=" + name, "generation=" + record.generation.number);
        }
    }

    private void closeOwed(Drain record) {
        for (VfsSession session : record.closeOwed) {
            try {
                session.closeAfterAbort();
            } catch (RuntimeException | Error failure) {
                health.namedLog().debug("Could not close a session. " + failure.getClass().getSimpleName());
            }
        }
        record.closeOwed.clear();
    }

    public void resume() {
        Drain stopped;
        synchronized (this) {
            if (state == State.DRAINING) {
                Drain active = drain;
                active.resumesWaiting++;
                try {
                    await(() -> active.done);
                } finally {
                    active.resumesWaiting--;
                    notifyAll();
                }
                if (active.generation != current) {
                    return;
                }
            }
            if (state == State.RUNNING) {
                return;
            }
            stopped = drain;
            try {
                current = build(current.number + 1);
            } catch (RuntimeException failure) {
                health.namedLog().error(Utils.withTrace("Could not resume the endpoint. "
                        + failure.getClass().getSimpleName(), failure));
                return;
            }
            synchronized (health) {
                if (health.observesRuntime(observer)) {
                    health.resumed();
                }
            }
            state = State.RUNNING;
        }
        closeOwed(stopped);
    }

    private void await(BooleanSupplier ready) {
        boolean interrupted = false;
        while (!ready.getAsBoolean()) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
