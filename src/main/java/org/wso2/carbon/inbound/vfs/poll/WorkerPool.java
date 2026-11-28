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

import org.wso2.carbon.inbound.vfs.conn.VfsSession;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongFunction;

public final class WorkerPool {
    public final class Slot {
        private final int index;
        private final VfsSession session;
        private final Thread thread;
        private boolean reserved;
        private boolean dispatching;
        private boolean done;
        private FileTask file;
        private CompletableFuture<FileTask.Outcome> outcome;

        private Slot(int index, VfsSession session) {
            this.index = index;
            this.session = session;
            thread = new Thread(this::work, "vfs-inbound-" + endpoint + "-worker-" + index);
            thread.setDaemon(true);
        }

        public void cancel() {
            synchronized (WorkerPool.this) {
                if (!reserved || dispatching || file != null) {
                    throw new IllegalStateException("The worker has no unassigned reservation.");
                }
                release(this);
            }
        }

        /** The caller keeps the file and its in flight count when handover returns null. */
        public CompletableFuture<FileTask.Outcome> submit(FileTask task) {
            Objects.requireNonNull(task);
            synchronized (WorkerPool.this) {
                if (!reserved || dispatching || file != null) {
                    throw new IllegalStateException("The worker has no unassigned reservation.");
                }
                dispatching = true;
            }
            CompletableFuture<FileTask.Outcome> answer = new CompletableFuture<>();
            boolean admitted = false;
            try {
                admitted = generation.lifecycle.handOver(generation, session, thread);
            } finally {
                synchronized (WorkerPool.this) {
                    dispatching = false;
                    if (!admitted) {
                        release(this);
                    } else {
                        file = task;
                        outcome = answer;
                    }
                    WorkerPool.this.notifyAll();
                }
            }
            return admitted ? answer : null;
        }

        private void work() {
            while (true) {
                FileTask task;
                CompletableFuture<FileTask.Outcome> answer;
                synchronized (WorkerPool.this) {
                    while (file == null && (!done || dispatching)) {
                        try {
                            WorkerPool.this.wait();
                        } catch (InterruptedException interrupted) {
                            if (done && !dispatching && file == null) {
                                return;
                            }
                        }
                    }
                    if (file == null) {
                        return;
                    }
                    task = file;
                    answer = outcome;
                }
                FileTask.Outcome result = null;
                Throwable failure = null;
                try {
                    result = task.process(session, null, false);
                } catch (RuntimeException | Error problem) {
                    failure = problem;
                } finally {
                    try {
                        generation.lifecycle.workerEnded(generation, session, failure instanceof Error);
                    } finally {
                        synchronized (WorkerPool.this) {
                            file = null;
                            outcome = null;
                            if (failure instanceof Error) {
                                done = true;
                            }
                            release(this);
                        }
                        if (failure == null) {
                            answer.complete(result);
                        } else {
                            answer.completeExceptionally(failure);
                        }
                    }
                }
                if (failure instanceof Error) {
                    replace(this);
                    throw (Error) failure;
                }
            }
        }
    }

    private final String endpoint;
    private final Generation generation;
    private final LongFunction<VfsSession> factory;
    private final List<Slot> workers = new ArrayList<>();
    private final ArrayDeque<Slot> free = new ArrayDeque<>();
    private boolean admitting = true;
    private RuntimeException replacementFailure;

    WorkerPool(String endpoint, Generation generation, List<VfsSession> sessions, LongFunction<VfsSession> factory) {
        this.endpoint = endpoint;
        this.generation = generation;
        this.factory = factory;
        for (VfsSession session : sessions) {
            Slot slot = new Slot(workers.size() + 1, session);
            workers.add(slot);
            free.add(slot);
        }
    }

    void start() {
        try {
            for (Slot slot : workers) {
                slot.thread.start();
            }
        } catch (RuntimeException | Error failure) {
            retire();
            throw failure;
        }
    }

    public synchronized Slot reserve() throws InterruptedException {
        while (admitting && generation.lifecycle.state() == Lifecycle.State.RUNNING) {
            if (replacementFailure != null) {
                throw replacementFailure;
            }
            Slot slot = free.poll();
            if (slot != null) {
                slot.reserved = true;
                return slot;
            }
            wait(250);
        }
        return null;
    }

    private void release(Slot slot) {
        slot.reserved = false;
        if (admitting && !slot.done) {
            free.add(slot);
        }
        notifyAll();
    }

    synchronized void stopAdmissions() {
        admitting = false;
        notifyAll();
    }

    synchronized void retire() {
        admitting = false;
        free.clear();
        for (Slot slot : workers) {
            slot.done = true;
        }
        notifyAll();
    }

    private void replace(Slot dead) {
        if (generation.isRetired() || generation.lifecycle.state() != Lifecycle.State.RUNNING) {
            return;
        }
        VfsSession replacement;
        try {
            replacement = factory.apply(generation.number);
        } catch (RuntimeException failure) {
            synchronized (this) {
                replacementFailure = failure;
                notifyAll();
            }
            return;
        }
        if (!generation.lifecycle.registerWorker(generation, replacement)) {
            replacement.closeAfterAbort();
            return;
        }
        Slot next = new Slot(dead.index, replacement);
        synchronized (this) {
            if (admitting) {
                workers.set(dead.index - 1, next);
                free.add(next);
                next.thread.start();
                notifyAll();
            }
        }
    }
}

