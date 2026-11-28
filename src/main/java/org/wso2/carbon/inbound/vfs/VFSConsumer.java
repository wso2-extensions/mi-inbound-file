/*
 *  Copyright (c) 2025, WSO2 LLC. (https://www.wso2.com).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.wso2.carbon.inbound.vfs;

import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.util.GracefulShutdownTimer;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.FileTask;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.poll.Lifecycle;
import org.wso2.carbon.inbound.vfs.poll.PollCycle;
import org.wso2.carbon.inbound.vfs.poll.ReadErrorCounter;
import org.wso2.carbon.inbound.vfs.processor.MoveTargetSpec;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.health.HealthRegistry;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.lock.ShareLock;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class VFSConsumer extends GenericPollingConsumer {

    private final VFSConfig vfsConfig;
    private final EndpointHealth health;
    private final Map<String, MoveTargetSpec> moveTargets;
    private final Lifecycle lifecycle;
    private final ReadErrorCounter readErrors;
    private final FileInjectHandler inject;
    private final String sequenceName;
    private PollCycle cycle;
    private ShareLock locks;
    private FailStateStore store;

    public VFSConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment, long scanInterval,
                       String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        if (scanInterval < 0) {
            throw new IllegalArgumentException("Invalid value for interval.");
        }
        vfsConfig = configure(properties, name, synapseEnvironment, injectingSeq, sequential);
        health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, scanInterval);
        moveTargets = moveTargets();
        readErrors = ReadErrorCounter.forEndpoint(health, vfsConfig.getMaxReadTransportErrors());
        sequenceName = injectingSeq;
        inject = new FileInjectHandler(injectingSeq, onErrorSeq, synapseEnvironment, vfsConfig);
        lifecycle = new Lifecycle(name, health, this::sessions, this::stopTimeout,
                sequential ? 0 : vfsConfig.getWorkerPoolSize(), this::newSession);
    }

    public VFSConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment, String cronExpression,
                       String injectingSeq, String onErrorSeq, boolean coordination, boolean sequential) {
        super(properties, name, synapseEnvironment, cronExpression, injectingSeq, onErrorSeq, coordination, sequential);
        if (cronExpression == null || cronExpression.trim().isEmpty()) {
            throw new IllegalArgumentException("A cron expression is required.");
        }
        vfsConfig = configure(properties, name, synapseEnvironment, injectingSeq, sequential);
        health = HealthRegistry.register(HealthRegistry.Kind.INBOUND, name, scanInterval);
        moveTargets = moveTargets();
        readErrors = ReadErrorCounter.forEndpoint(health, vfsConfig.getMaxReadTransportErrors());
        sequenceName = injectingSeq;
        inject = new FileInjectHandler(injectingSeq, onErrorSeq, synapseEnvironment, vfsConfig);
        lifecycle = new Lifecycle(name, health, this::sessions, this::stopTimeout,
                sequential ? 0 : vfsConfig.getWorkerPoolSize(), this::newSession);
    }

    private static VFSConfig configure(Properties properties, String name, SynapseEnvironment environment,
                                       String sequence, boolean sequential) {
        if (sequence == null || sequence.trim().isEmpty()) {
            throw new IllegalArgumentException("An injecting sequence name is required.");
        }
        TestPoints.copy("inbound", VFSConsumer.class, EndpointHealth.class);
        return new VFSConfig(properties, environment, sequential, name);
    }

    private Map<String, MoveTargetSpec> moveTargets() {
        Map<String, MoveTargetSpec> targets = new HashMap<>();
        addTarget(targets, VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS, vfsConfig.getMoveAfterProcess());
        addTarget(targets, VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE, vfsConfig.getMoveAfterFailure());
        addTarget(targets, VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, vfsConfig.getMoveAfterMoveFailure());
        return Collections.unmodifiableMap(targets);
    }

    private void addTarget(Map<String, MoveTargetSpec> targets, String parameter, String uri) {
        if (uri != null) {
            targets.put(parameter, MoveTargetSpec.from(parameter, uri, vfsConfig, health.namedLog()));
        }
    }

    MoveTargetSpec moveTarget(String parameter) {
        return moveTargets.get(parameter);
    }

    VfsSession newSession(long number) {
        return new VfsSession(name, number, vfsConfig, scanInterval, health.namedLog());
    }

    private List<VfsSession> sessions(long number) {
        VfsSession session = newSession(number);
        try {
            ShareLock newLocks = ShareLock.create(name, vfsConfig.isAutoLockRelease(), vfsConfig.getLockStaleAge(),
                    health);
            FailStateStore.Parameters parameters = vfsConfig.getFailStateParameters();
            MoveTargetSpec fallback = moveTarget(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE);
            if (fallback != null) {
                String root = Utils.extractPath(Utils.stripVfsSchemeIfPresent(vfsConfig.getFileURI()));
                parameters.moveAfterFailedMove = id -> fallbackTarget(fallback, root, id.uri);
            }
            // Looked up by name at each generation: a kept store comes back with its pending writes, and after a
            // release the consumer uses the new registered store.
            store = FailStateStore.forEndpoint(name, parameters, newLocks, health);
            locks = newLocks;
            cycle = new PollCycle(new FileTask.Services(name, vfsConfig, locks, store, readErrors, inject,
                    moveTargets, health), scanInterval, sequenceName);
            return Collections.singletonList(session);
        } catch (RuntimeException | Error failure) {
            session.closeAfterAbort();
            throw failure;
        }
    }

    static String fallbackTarget(MoveTargetSpec fallback, String root, String uri) {
        String rootPath = sourcePath(root);
        if (rootPath.endsWith("/*")) {
            rootPath = rootPath.substring(0, rootPath.length() - 1);
        }
        String filePath = sourcePath(uri);
        int last = filePath.lastIndexOf('/');
        String base = filePath.substring(last + 1);
        String relative = "";
        if (!rootPath.equals(filePath)) {
            String prefix = rootPath.endsWith("/") ? rootPath : rootPath + "/";
            if (!filePath.startsWith(prefix)) {
                throw new IllegalArgumentException("The source file is outside its configured folder.");
            }
            if (last >= prefix.length()) {
                relative = "/" + filePath.substring(prefix.length(), last);
            }
        }
        return fallback.target(relative, base, System.currentTimeMillis()).recordedPath;
    }

    private static String sourcePath(String uri) {
        String path = Utils.extractPath(Utils.stripVfsSchemeIfPresent(uri));
        path = path.replace('\\', '/');
        int scheme = path.indexOf("://");
        if (scheme >= 0) {
            int slash = path.indexOf('/', scheme + 3);
            path = slash < 0 ? "/" : path.substring(slash);
        } else if (path.regionMatches(true, 0, "file:", 0, 5)) {
            path = path.substring(5);
        }
        if (path.length() >= 3 && path.charAt(0) == '/' && path.charAt(2) == ':'
                && Character.isLetter(path.charAt(1))) {
            path = path.substring(1);
        }
        return path;
    }

    private long stopTimeout() {
        GracefulShutdownTimer timer = GracefulShutdownTimer.getInstance();
        if (timer.isStarted()) {
            return timer.getRemainingTimeMillis();
        }
        if (synapseEnvironment instanceof Axis2SynapseEnvironment) {
            ConfigurationContext context = ((Axis2SynapseEnvironment) synapseEnvironment).getAxis2ConfigurationContext();
            if (context != null && "SHUTTING_DOWN".equals(context.getProperty("local_current.server.status"))) {
                return 0;
            }
        }
        return vfsConfig.getStopTimeout();
    }

    public Map<String, Object> getHealth() {
        Map<String, Object> snapshot = health.snapshot();
        EndpointHealth.State state = EndpointHealth.State.valueOf((String) snapshot.get("state"));
        snapshot.put("status", state.status());
        return snapshot;
    }

    @Override
    public Object poll() {
        Generation generation;
        PollCycle active;
        synchronized (lifecycle) {
            generation = lifecycle.enterPoll();
            active = cycle;
        }
        if (generation != null) {
            try {
                // The deployer can schedule this consumer before publishing its endpoint.
                SynapseConfiguration configuration = synapseEnvironment == null ? null
                        : synapseEnvironment.getSynapseConfiguration();
                if (configuration != null && configuration.getInboundEndpoint(name) != null) {
                    active.run(generation);
                }
            } finally {
                lifecycle.exitPoll(generation);
            }
        }
        return null;
    }

    @Override
    public void resume() {
        lifecycle.resume();
    }

    @Override
    public void pause() {
        lifecycle.stop("pause");
    }

    @Override
    public void destroy() {
        lifecycle.stop("destroy");
        // The next deploy of this name in the same class loader then builds a new store with its new values. The
        // store stays while a worker is still out or a write is pending, so no file is processed twice.
        lifecycle.whenStoppedWithNoWork(() -> {
            if (store != null) {
                FailStateStore.release(name, store);
            }
        });
    }
}
