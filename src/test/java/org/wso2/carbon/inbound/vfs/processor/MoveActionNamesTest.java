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
package org.wso2.carbon.inbound.vfs.processor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.cache.NullFilesCache;
import org.wso2.org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.wso2.vfs.shared.log.NamedLog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;

class MoveActionNamesTest {
    @TempDir Path folder;

    // The temporary names of a move are resolved with no probe of the server. They must be the names a resolve of the
    // session gives for the same address, for every file name: a cut or changed name could be a real file.
    @Test
    void temporaryNamesAreTheNamesOfAResolveOfTheSession() throws Exception {
        Path done = Files.createDirectories(folder.resolve("done"));
        Properties settings = new Properties();
        settings.setProperty(TRANSPORT_FILE_FILE_URI, folder.resolve("in").toUri().toString());
        String destinationUri = done.toUri().toString();
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS, destinationUri);
        MoveTargetSpec spec = MoveTargetSpec.from(TRANSPORT_FILE_MOVE_AFTER_PROCESS, destinationUri,
                new VFSConfig(settings), mock(NamedLog.class));
        StandardFileSystemManager manager = new StandardFileSystemManager();
        manager.setFilesCache(new NullFilesCache());
        manager.init();
        try {
            MoveTargetSpec.Target target = spec.target("", "f.dat", 0);
            FileObject destination = manager.resolveFile(Utils.stripVfsSchemeIfPresent(target.fileUri));
            for (String name : new String[] {"a?b.dat", "a#b.dat", "a%41.dat", "a\\b.dat", "file:1.dat", "a b.dat",
                    ":x.dat"}) {
                String uri = target.sibling(name);
                FileName session = nameOrNull(() -> manager.resolveFile(Utils.stripVfsSchemeIfPresent(uri)));
                FileName unprobed = nameOrNull(() -> MoveAction.unprobed(destination, uri));
                if (session == null) {
                    assertNull(unprobed, name);
                    continue;
                }
                assertEquals(session.getURI(), unprobed.getURI(), name);
                assertEquals(session.getPath(), unprobed.getPath(), name);
            }
        } finally {
            manager.close();
        }
    }

    private interface Resolve {
        FileObject get() throws FileSystemException;
    }

    // The name of the resolved object, or null when the address can not be resolved.
    private static FileName nameOrNull(Resolve resolve) {
        try {
            return resolve.get().getName();
        } catch (FileSystemException refused) {
            return null;
        }
    }
}
