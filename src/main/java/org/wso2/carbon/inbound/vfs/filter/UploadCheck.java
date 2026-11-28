/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.logging.LogFactory;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.vfs.shared.error.Classification;
import org.wso2.vfs.shared.error.ErrorClassifier;
import org.wso2.vfs.shared.failstate.DigestResult;
import org.wso2.vfs.shared.failstate.FileDigest;
import org.wso2.vfs.shared.lock.LockIo;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.io.IOException;

public final class UploadCheck {
    private static final ErrorClassifier CLASSIFIER = new ErrorClassifier(NamedLog.inbound(
            LogFactory.getLog(UploadCheck.class), "upload", new RepeatLimiter()));

    public interface Wait {
        boolean await(long millis) throws InterruptedException;
    }

    public static final class Result {
        public final boolean stable;
        public final Classification failure;

        private Result(boolean stable, Classification failure) {
            this.stable = stable;
            this.failure = failure;
        }
    }

    public static Result check(LockIo io, String uri, long interval, Wait wait)
            throws IOException, InterruptedException {
        if (interval <= 0) {
            return new Result(true, null);
        }
        long size;
        long modified;
        try (FileObject first = io.resolve(uri)) {
            size = first.getContent().getSize();
            modified = modified(first);
        }
        DigestResult first = FileDigest.of(io, uri);
        if (first.kind != DigestResult.Kind.DONE || !wait.await(interval)) {
            return new Result(false, first.failure);
        }
        try (FileObject fresh = io.resolve(uri)) {
            fresh.refresh();
            if (size != fresh.getContent().getSize() || modified != modified(fresh)) {
                return new Result(false, null);
            }
        }
        DigestResult second = FileDigest.of(io, uri);
        return new Result(second.kind == DigestResult.Kind.DONE && first.digest.equals(second.digest), second.failure);
    }

    // A time that the server does not send is unknown on both reads. The size and the digests still decide, and the
    // old transport used the digests alone. A transport error is thrown as before.
    private static long modified(FileObject file) throws FileSystemException {
        try {
            return file.getContent().getLastModifiedTime();
        } catch (FileSystemException unknown) {
            if (CLASSIFIER.classify(unknown, 0, ErrorClassifier.CallType.READ).getKind().isTransport()) {
                throw unknown;
            }
            return 0;
        }
    }

    private UploadCheck() {
    }
}
