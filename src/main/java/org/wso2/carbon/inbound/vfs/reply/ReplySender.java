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
package org.wso2.carbon.inbound.vfs.reply;

import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundResponseSender;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileType;
import org.wso2.org.apache.commons.vfs2.NameScope;
import org.wso2.org.apache.commons.vfs2.provider.CommitStep;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.lock.LockOutcome;
import org.wso2.vfs.shared.lock.ShareLock;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.write.AtomicWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS;
import static org.apache.synapse.commons.vfs.VFSConstants.ERROR_CODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FILE_NAME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.REPLY_FILE_NAME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.REPLY_FILE_URI;

public final class ReplySender implements InboundResponseSender {

    // The lock wait of a write of the file connector: three tries, 5 s apart, so 10 s in all.
    private static final int LOCK_TRIES = 3;
    private static final long LOCK_GAP_MILLIS = 5000;

    private final Thread owner = Thread.currentThread();
    private final VFSConfig config;
    private final VfsSession session;
    private final Generation generation;
    private final AtomicWriter writer;
    private final ShareLock locks;
    private final NamedLog log;
    private volatile boolean failed;
    private Classification transportFailure;
    private boolean resetUsable = true;

    public ReplySender(VFSConfig config, VfsSession session, Generation generation, AtomicWriter writer,
                       ShareLock locks, NamedLog log) {
        this.config = config;
        this.session = session;
        this.generation = generation;
        this.writer = writer;
        this.locks = locks;
        this.log = log;
    }

    public boolean failed() {
        return failed;
    }

    public boolean resetUsable() {
        return resetUsable;
    }

    public Classification transportFailure() {
        return transportFailure;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void sendBack(MessageContext message) {
        org.apache.axis2.context.MessageContext axis = ((Axis2MessageContext) message).getAxis2MessageContext();
        Map<String, Object> headers = (Map<String, Object>) axis.getProperty(TRANSPORT_HEADERS);
        if (headers == null) {
            headers = new HashMap<>();
            axis.setProperty(TRANSPORT_HEADERS, headers);
        }
        String uri = config.getReplyFileURI();
        if (uri == null) {
            throw failure(headers, "ReplyFileURI is not set.");
        }
        if (Thread.currentThread() != owner) {
            throw failure(headers, "The reply needs mediation on the injecting thread.");
        }
        if (generation.isRetired()) {
            throw failure(headers, "The file generation has retired.");
        }

        uri = Utils.extractPath(Utils.stripVfsSchemeIfPresent(uri));
        long start = System.nanoTime();
        try {
            LockIo io = session.lockIo(REPLY_FILE_URI);
            try (FileObject location = io.resolve(uri)) {
                String target = uri;
                if (location.getType() == FileType.FOLDER) {
                    Object name = headers.get(REPLY_FILE_NAME);
                    String replyName = name == null ? config.getReplayFileName() : name.toString();
                    target = (uri.endsWith("/") ? uri : uri + "/") + replyName;
                    try (FileObject child = io.resolve(target)) {
                        if (!location.getName().isDescendent(child.getName(), NameScope.DESCENDENT)) {
                            throw new IOException("The reply file must be under the reply folder.");
                        }
                    }
                }
                MessageFormatter formatter = MessageProcessorSelector.getMessageFormatter(axis);
                OMOutputFormat format = BaseUtils.getOMOutputFormat(axis);
                AtomicWriter.Content payload = out -> formatter.writeTo(axis, format, out, false);
                if (config.isAppend()) {
                    append(io, target, payload);
                } else {
                    AtomicWriter.Result result = writer.write(io, target, CommitStep.Mode.OVERWRITE, payload,
                            config.isFileLocking(), LOCK_TRIES, LOCK_GAP_MILLIS);
                    if (result.connectionLost()) {
                        if (result.outcome != AtomicWriter.Outcome.COMMITTED) {
                            throw lost();
                        }
                        recordLoss(uri, lost(), start);
                    }
                    if (result.outcome != AtomicWriter.Outcome.COMMITTED) {
                        throw new IOException("Reply write returned " + result.outcome + ".");
                    }
                }
            }
        } catch (IOException | RuntimeException error) {
            recordLoss(uri, error, start);
            // The reply address is not named on purpose: it is the text the user wrote, and the mask of a line can
            // not hide every password in it. The line names the source file of the reply.
            Object relative = headers.get("RELATIVE_PATH");
            Object name = headers.get(FILE_NAME);
            throw failure(headers, "Could not write the reply file.", relative == null || name == null
                    ? "Could not write the reply file."
                    : "Could not write the reply file for " + relative + "/" + name + ".");
        }
    }

    private void recordLoss(String uri, Throwable error, long start) {
        Classification reason = new ErrorClassifier(log).classify(error,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start), ErrorClassifier.CallType.CHANGE);
        if (reason.getKind().isTransport()) {
            transportFailure = reason;
            generation.transportFault(reason);
            resetUsable = session.resetCurrentLocation(REPLY_FILE_URI, uri, error);
        }
    }

    private void append(LockIo io, String uri, AtomicWriter.Content payload) throws IOException {
        LockOutcome lock = config.isFileLocking()
                ? locks.acquireWithWait(io, uri, ShareLock.Mode.WRITER, LOCK_TRIES, LOCK_GAP_MILLIS)
                : locks.enterLocalOnly(uri, LOCK_TRIES, LOCK_GAP_MILLIS);
        if (lock.kind != LockOutcome.Kind.ACQUIRED) {
            if (lock.connectionLost() || lock.classification != null && lock.classification.getKind().isTransport()) {
                throw lost();
            }
            throw new IOException("The reply file lock is unavailable.");
        }
        boolean lost = false;
        boolean written = false;
        long start = System.nanoTime();
        try {
            try (FileObject file = io.resolve(uri); OutputStream out = file.getContent().getOutputStream(true)) {
                payload.writeTo(out);
            }
            written = true;
        } catch (IOException | RuntimeException error) {
            lost = new ErrorClassifier(log).classify(error, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                    ErrorClassifier.CallType.CHANGE).getKind().isTransport();
            throw error;
        } finally {
            if (lost) {
                lock.handle.abandon();
            } else {
                LockOutcome end = lock.handle.releaseChecked(io);
                if (end.connectionLost() || end.classification != null && end.classification.getKind().isTransport()) {
                    if (written) {
                        recordLoss(uri, lost(), start);
                    } else {
                        throw lost();
                    }
                }
            }
        }
    }

    private static FileSystemException lost() {
        FileSystemException error = new FileSystemException("The reply connection was lost.");
        error.setFault(Fault.TRANSPORT);
        return error;
    }

    private SynapseException failure(Map<String, Object> headers, String message) {
        return failure(headers, message, message);
    }

    // The line may name the reply file. The error code and the exception keep the text with no address.
    private SynapseException failure(Map<String, Object> headers, String message, String line) {
        failed = true;
        headers.put(ERROR_CODE, message);
        log.error(line);
        return new SynapseException(message);
    }
}
