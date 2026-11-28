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
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.vfs.shared.log.NamedLog;

import java.util.Properties;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.wso2.carbon.inbound.vfs.VFSConstants.*;

class MoveTargetSpecTest {
    @Test
    void computesIndependentNamesAndKeepsTheQueryOnEveryTarget() {
        Properties settings = new Properties();
        settings.setProperty(TRANSPORT_FILE_FILE_URI, "file:///in/*?transport.vfs.CreateFolder=false");
        settings.setProperty(FORCE_CREATE_FOLDER, "true");
        String uri = "vfs:file:///done/*?transport.vfs.IsMounted=true&x=abc";
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS, uri);
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_FAILURE, uri);
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, uri);
        settings.setProperty("transport.vfs.MoveTimestampFormat", "'prefix_'");
        settings.setProperty("transport.vfs.SubFolderTimestampFormat", "'date'");
        VFSConfig config = new VFSConfig(settings);
        NamedLog log = mock(NamedLog.class);
        MoveTargetSpec success = MoveTargetSpec.from(TRANSPORT_FILE_MOVE_AFTER_PROCESS, uri, config, log);
        String query = "?transport.vfs.IsMounted=true&x=abc";
        assertTrue(success.mounted);
        assertTrue(success.createFolder);
        IntStream.range(0, 16).parallel().forEach(number -> {
            MoveTargetSpec.Target target = success.target("/a", number + ".csv", 123456789);
            assertEquals("file:///done/date" + query, target.folderUri);
            assertEquals("file:///done/date/a" + query, target.parentUri);
            assertEquals("file:///done/date/a/prefix_" + number + ".csv" + query, target.fileUri);
            assertEquals("date/a/prefix_" + number + ".csv", target.recordedPath);
            assertEquals(target.fileUri, success.recorded("/a", target.recordedPath).fileUri);
        });
        assertThrows(IllegalArgumentException.class, () -> success.recorded("", "/external.csv"));
        assertThrows(IllegalArgumentException.class, () -> success.recorded("", "file:///outside.csv"));
        assertThrows(IllegalArgumentException.class, () -> success.recorded("", "%2e%2e/outside.csv"));
        for (String parameter : new String[] {TRANSPORT_FILE_MOVE_AFTER_FAILURE,
                TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE}) {
            MoveTargetSpec spec = MoveTargetSpec.from(parameter, uri, config, log);
            assertEquals("file:///done/prefix_bad.csv" + query, spec.target("", "bad.csv", 0).fileUri);
        }
    }

    @Test
    void addsTheDateToTheFolderWithNoSeparatorAndReadsTheRecordedPathBack() {
        String[][] rows = {
                {"file:///done", "'day'", "file:///doneday", "doneday"},
                {"file:///done", "'/day'", "file:///done/day", "day"},
                {"file:///done/", "'day'", "file:///done/day", "day"},
                {"file:///done/", "'/day'", "file:///done/day", "day"},
                {"file:///done/*", "'day'", "file:///done/day", "day"},
        };
        for (String[] row : rows) {
            String uri = row[0] + "?x=1";
            Properties settings = new Properties();
            settings.setProperty(TRANSPORT_FILE_FILE_URI, "file:///in");
            settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS, uri);
            settings.setProperty("transport.vfs.SubFolderTimestampFormat", row[1]);
            MoveTargetSpec spec = MoveTargetSpec.from(TRANSPORT_FILE_MOVE_AFTER_PROCESS, uri,
                    new VFSConfig(settings), mock(NamedLog.class));
            for (String relative : new String[] {"", "/sub"}) {
                String label = row[0] + " " + row[1] + " " + relative;
                MoveTargetSpec.Target target = spec.target(relative, "a.csv", 0);
                assertEquals(row[2] + "?x=1", target.folderUri, label);
                assertEquals(row[2] + relative + "/a.csv?x=1", target.fileUri, label);
                assertEquals(row[3] + relative + "/a.csv", target.recordedPath, label);
                MoveTargetSpec.Target recorded = spec.recorded(relative, target.recordedPath);
                assertEquals(target.folderUri, recorded.folderUri, label);
                assertEquals(target.parentUri, recorded.parentUri, label);
                assertEquals(target.fileUri, recorded.fileUri, label);
            }
            if (row[3].startsWith("done")) {
                // A marker can not name a folder next to the move folder that the date can not give.
                assertThrows(IllegalArgumentException.class, () -> spec.recorded("", "other/a.csv"));
            }
        }
    }
    @Test
    void keepsEncodedNamesRelativeAcrossMarkerReadback() {
        String destination = "file:///done?transport.vfs.CreateFolder=false";
        Properties settings = new Properties();
        settings.setProperty(TRANSPORT_FILE_FILE_URI, "file:///input");
        settings.setProperty(TRANSPORT_FILE_MOVE_AFTER_PROCESS, destination);
        MoveTargetSpec spec = MoveTargetSpec.from(TRANSPORT_FILE_MOVE_AFTER_PROCESS, destination,
                new VFSConfig(settings), mock(NamedLog.class));

        MoveTargetSpec.Target plain = spec.target("", "daily:batch.csv", 0);
        assertEquals("daily%3Abatch.csv", plain.recordedPath);
        assertEquals(plain.fileUri, spec.recorded("", plain.recordedPath).fileUri);
        assertEquals("x%3Apart.csv", spec.target("", "x:part.csv", 0).recordedPath);
        assertEquals("1:part.csv", spec.recorded("", "1:part.csv").recordedPath);
        MoveTargetSpec.Target percent = spec.target("", "100%253A.csv", 0);
        assertEquals("100%253A.csv", percent.recordedPath);
        assertEquals(percent.fileUri, spec.recorded("", percent.recordedPath).fileUri);
        MoveTargetSpec.Target folder = spec.target("/daily:files/child", "a%25.csv", 0);
        assertEquals("daily%3Afiles/child/a%25.csv", folder.recordedPath);
        assertEquals(folder.parentUri, spec.recorded("/daily:files/child", folder.recordedPath).parentUri);
        assertEquals(folder.fileUri, spec.recorded("/daily:files/child", folder.recordedPath).fileUri);
        String existing = "20200913/nested/child/20200913175640_data.csv";
        assertEquals(existing, spec.recorded("/nested/child", existing).recordedPath);
        assertThrows(IllegalArgumentException.class, () -> spec.recorded("", "sftp://host/path"));
        assertThrows(IllegalArgumentException.class, () -> spec.recorded("", "file:/tmp/source"));
        assertThrows(IllegalArgumentException.class, () -> spec.recorded("", "/tmp/source"));
    }
}
