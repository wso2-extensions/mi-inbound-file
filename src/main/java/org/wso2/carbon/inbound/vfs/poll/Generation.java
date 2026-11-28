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
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.health.EndpointHealth;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongFunction;

public final class Generation {

    public final long number;
    public final VfsSession pollSession;
    final List<VfsSession> sessions;
    public final WorkerPool workerPool;
    final Set<VfsSession> inside = Collections.newSetFromMap(new IdentityHashMap<>());
    volatile boolean retired;
    boolean pollActive;
    int inFlight;
    final Lifecycle lifecycle;
    private final EndpointHealth endpoint;

    Generation(long number, List<VfsSession> sessions, Lifecycle lifecycle, EndpointHealth endpoint,
               String name, int workerCount, LongFunction<VfsSession> factory) {
        this.number = number;
        this.sessions = new CopyOnWriteArrayList<>(sessions);
        this.pollSession = sessions.get(0);
        this.lifecycle = lifecycle;
        this.endpoint = endpoint;
        workerPool = workerCount == 0 ? null : new WorkerPool(name, this,
                this.sessions.subList(this.sessions.size() - workerCount, this.sessions.size()), factory);
        if (workerPool != null) {
            workerPool.start();
        }
    }

    public boolean isRetired() {
        return retired;
    }

    boolean hasWork() {
        return pollActive || inFlight != 0;
    }

    private void update(Consumer<EndpointHealth> change) {
        synchronized (lifecycle) {
            if (!retired) {
                change.accept(endpoint);
            }
        }
    }

    public void pollStarted() {
        update(EndpointHealth::pollStarted);
    }

    public void pollSkipped() {
        update(EndpointHealth::pollSkipped);
    }

    public void pollFinished(boolean rootReached, Classification failure) {
        update(health -> health.pollFinished(rootReached, failure));
    }

    public void progress(long bytes) {
        synchronized (lifecycle) {
            if (!retired) {
                endpoint.progress(bytes);
            }
        }
    }

    public void fileDone() {
        update(EndpointHealth::fileDone);
    }

    public void fileFailed(Classification failure) {
        update(health -> health.fileFailed(failure));
    }

    public void transportFault(Classification failure) {
        update(health -> health.transportFault(failure));
    }

    public void inferredMove() {
        update(EndpointHealth::inferredMove);
    }

    public void openSessions(int count) {
        update(health -> health.openSessions(count));
    }

    public void flag(EndpointHealth.Flag flag, String path) {
        update(health -> health.flag(flag, path));
    }

    public void clearFlag(EndpointHealth.Flag flag, String path) {
        update(health -> health.clearFlag(flag, path));
    }
}
