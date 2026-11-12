/*
 *  Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
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

import org.wso2.org.apache.commons.vfs2.FileNotFoundException;

/**
 * Thrown when a polled file is no longer available when it is read (for example, it was moved by another process).
 * As in the Synapse VFS transport and the MI file inbound endpoint, no post-processing action is run for it.
 */
public class FileNotAvailableException extends RuntimeException {

    // guards against cause chains that loop back on themselves
    private static final int MAX_CAUSE_DEPTH = 20;

    public FileNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Whether the error, or one of its causes, is a VFS {@link FileNotFoundException}.
     */
    public static boolean isFileNotFound(Throwable e) {
        int depth = 0;
        for (Throwable t = e; t != null && depth < MAX_CAUSE_DEPTH; t = t.getCause(), depth++) {
            if (t instanceof FileNotFoundException || t instanceof FileNotAvailableException) {
                return true;
            }
        }
        return false;
    }
}
