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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider;
import org.wso2.vfs.shared.lock.LockIo;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class UploadCheckTest {
    @TempDir Path folder;

    // Some SFTP servers send no time. The check of the old transport compared digests only, so a file with no time
    // is judged by its digests here too.
    @Test
    void aFileWithNoTimeIsJudgedByItsDigests() throws Exception {
        Path file = Files.write(folder.resolve("upload.txt"), "settled".getBytes(StandardCharsets.UTF_8));
        DefaultFileSystemManager manager = new DefaultFileSystemManager();
        manager.addProvider("file", new DefaultLocalFileProvider());
        manager.addProvider("notime", new DefaultLocalFileProvider() {
            @Override
            public FileObject findFile(FileObject base, String uri, FileSystemOptions options)
                    throws FileSystemException {
                FileObject real = manager.resolveFile("file" + uri.substring("notime".length()), options);
                FileObject timeless = spy(real);
                FileContent content = spy(real.getContent());
                doThrow(new FileSystemException("Last modification time not fetched."))
                        .when(content).getLastModifiedTime();
                doReturn(content).when(timeless).getContent();
                return timeless;
            }
        });
        manager.init();
        try {
            String uri = "notime" + file.toUri().toString().substring("file".length());
            UploadCheck.Result result = UploadCheck.check(new LockIo(manager, null), uri, 1, millis -> true);
            assertTrue(result.stable);
            assertNull(result.failure);
        } finally {
            manager.close();
        }
    }
}
