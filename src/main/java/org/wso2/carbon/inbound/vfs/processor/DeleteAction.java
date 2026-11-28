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

package org.wso2.carbon.inbound.vfs.processor;

import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.AtomicOps;
import org.wso2.org.apache.commons.vfs2.provider.Fault;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.failstate.ActionResult;

import java.util.concurrent.TimeUnit;

import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.CHANGE;
import static org.wso2.vfs.shared.error.ErrorClassifier.CallType.READ;

public final class DeleteAction {
    private final ErrorClassifier classifier;

    public DeleteAction(ErrorClassifier classifier) {
        this.classifier = classifier;
    }

    /** Returns null when the delete is settled. */
    public ActionResult execute(VfsSession session, FileObject source, String sourceUri) {
        return delete(session, source, sourceUri, ActionResult.Step.NONE, null);
    }

    ActionResult delete(VfsSession session, FileObject source, String sourceUri, ActionResult.Step step, String temp) {
        boolean committed = step == ActionResult.Step.DELETE_SOURCE;
        long start = System.nanoTime();
        try {
            if (source.delete()) {
                return null;
            }
            FileSystemException refused = new FileSystemException("The source file could not be deleted.");
            refused.setFault(Fault.APPLICATION);
            return new ActionResult(step, true, temp, committed, classifier.classify(refused, elapsed(start), CHANGE));
        } catch (FileSystemException error) {
            Classification failure = classifier.classify(error, elapsed(start), CHANGE);
            boolean lost = failure.getKind().isTransport();
            if (lost && session.reset(source.getFileSystem(), error) && failure.isFast() && !committed) {
                FileObject fresh = null;
                long look = System.nanoTime();
                try {
                    fresh = session.resolve(sourceUri, session.options(TRANSPORT_FILE_FILE_URI));
                    if (!AtomicOps.existsNow(fresh)) {
                        return null;
                    }
                } catch (FileSystemException unreadable) {
                    Classification observation = classifier.classify(unreadable, elapsed(look), READ);
                    if (observation.getKind().isTransport()) {
                        session.resetLocation(TRANSPORT_FILE_FILE_URI, sourceUri, fresh, unreadable);
                    }
                }
            }
            return new ActionResult(step, !lost, temp, committed, failure);
        } finally {
            session.remoteCallEnded();
        }
    }

    static long elapsed(long start) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }
}
