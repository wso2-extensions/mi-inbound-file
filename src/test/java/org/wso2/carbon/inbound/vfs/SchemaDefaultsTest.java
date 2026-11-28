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
package org.wso2.carbon.inbound.vfs;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileSystemConfigBuilder;

import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class SchemaDefaultsTest {
    @Test
    void publishedFieldsMatchTheInboundDefaults() throws Exception {
        Map<String, String> fields = new HashMap<>();
        try (InputStreamReader reader = new InputStreamReader(
                getClass().getResourceAsStream("/uischema.json"), StandardCharsets.UTF_8)) {
            for (JsonElement group : JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("elements")) {
                for (JsonElement entry : group.getAsJsonObject().getAsJsonObject("value").getAsJsonArray("elements")) {
                    JsonObject value = entry.getAsJsonObject().getAsJsonObject("value");
                    String name = value.get("name").getAsString();
                    assertNull(fields.put(name, value.has("defaultValue") ? value.get("defaultValue").getAsString() : ""), name);
                }
            }
        }
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, "sftp://localhost/in");
        VFSConfig config = new VFSConfig(properties);
        Map<String, Object> expected = new HashMap<>();
        expected.put("ContentType", config.getContentType());
        expected.put("FileNamePattern", config.getFileNamePattern());
        expected.put("Streaming", config.isStreaming());
        expected.put("Build", config.isBuild());
        expected.put("MaskHeaderPassword", config.isMaskHeaderPassword());
        expected.put("FailOnErrorCode", config.isFailOnErrorCode());
        expected.put("ReplyFileURI", config.getReplyFileURI());
        expected.put("ReplyFileName", config.getReplayFileName());
        expected.put("Append", config.isAppend());
        expected.put("FileProcessCount", config.getFileProcessingCount());
        expected.put("FileProcessInterval", config.getFileProcessingInterval());
        expected.put("FileSortAttribute", config.getFileSortParam());
        expected.put("FileSortAscending", config.isFileSortAscending());
        expected.put("WorkerPoolSize", config.getWorkerPoolSize());
        expected.put("MinimumAge", config.getMinimumAge());
        expected.put("MaximumAge", config.getMaximumAge());
        expected.put("MaxReadTransportErrors", config.getMaxReadTransportErrors());
        expected.put("CheckSizeIgnoreEmpty", config.getCheckSizeIgnoreEmpty());
        expected.put("MoveAfterProcess", config.getMoveAfterProcess());
        expected.put("MoveAfterFailure", config.getMoveAfterFailure());
        expected.put("MoveAfterFailedMove", config.getMoveAfterMoveFailure());
        expected.put("MoveTimestampFormat", config.getMoveTimestampPattern());
        expected.put("SubFolderTimestampFormat", config.getSubfolderTimestamp());
        expected.put("CreateFolder", config.isForceCreateFolder());
        expected.put("UpdateLastModified", config.isUpdateLastModified());
        expected.put("TempFolderName", config.getTempFolderName());
        expected.put("TempFileSuffix", config.getTempFileSuffix());
        expected.put("MaxRetryCount", config.getMaxRetryCount());
        expected.put("AutoLockRelease", config.isAutoLockRelease());
        expected.put("FailedRecordNextRetryDuration", config.getFailedRecordNextRetryDuration());
        expected.put("FailedRecordFlagAfterRetries", config.getFailedRecordFlagAfterRetries());
        expected.put("AvoidPermissionCheck", config.isAvoidPermissionCheck());
        expected.put("FileSizeLimit", (long) config.getFileSizeLimit());
        expected.put("Locking", config.isFileLocking() ? "enable" : "disable");
        expected.put("ReconnectTimeout", config.getReconnectTimeout() / 1000 + "s");
        expected.put("StopTimeout", config.getStopTimeout() / 1000 + "s");
        expected.put("LockReleaseSameNode", true);
        expected.put("IsMounted", Boolean.parseBoolean(config.getVfsSchemeProperties().get("transport.vfs.IsMounted")));
        // The endpoint sets no DFS value, so the client resolves DFS paths.
        assertFalse(config.getVfsSchemeProperties().containsKey("transport.vfs.SMB2DfsEnabled"));
        expected.put("SMB2DfsEnabled", Smb2FileSystemConfigBuilder.getInstance().getDfsEnabled(
                Utils.attachFileSystemOptions(config.getVfsSchemeProperties(), null)));
        assertEquals(VFSConfig.DELETE, config.getActionAfterProcess());
        assertEquals(VFSConfig.DELETE, config.getActionAfterFailure());
        expected.put("ActionAfterProcess", "DELETE");
        expected.put("ActionAfterFailure", "DELETE");
        assertEquals(0, config.getCheckSizeInterval());
        expected.put("CheckSizeInterval", null);
        // A UI endpoint sends no stale age by default, so a deprecated interval that the user sets still applies.
        assertEquals(900000, config.getLockStaleAge());
        for (String unset : ("FileURI FailedRecordsFileName FailedRecordsFileDestination MoveFailedRecordTimestampFormat "
                + "AutoLockReleaseInterval LockStaleAge waitTimeBeforeRead ResolveHostsDynamically ActionAfterErrors "
                + "MoveAfterErrors DistributedLock DistributedTimeout ClusterAware").split(" ")) {
            expected.put(unset, null);
        }
        for (String option : ("SFTPIdentities SFTPIdentityPassPhrase SFTPUserDirIsRoot SFTPSessionTimeout "
                + "SFTPKeepAliveCount SFTPConnectTimeout FTPConnectTimeout FTPSocketTimeout FTPDataTimeout "
                + "SMB2ReadTimeout SMB2WriteTimeout SMB2TransactTimeout WriteStallTimeout SFTPKeyExchangeAlgorithms "
                + "SFTPHostKeyAlgorithms SFTPCiphers SFTPMacs SFTPPubkeyAcceptedAlgorithms").split(" ")) {
            assertFalse(config.getVfsSchemeProperties().containsKey("transport.vfs." + option), option);
            expected.put(option, null);
        }
        expected.forEach((key, value) -> assertEquals(value == null ? "" : value.toString(),
                fields.remove("transport.vfs." + key), key));
        for (String key : ("vfs.passive vfs.implicit vfs.protection vfs.ssl.keystore vfs.ssl.truststore "
                + "vfs.ssl.kspassword vfs.ssl.tspassword vfs.ssl.keypassword filetype vfs.EncryptionEnabled "
                + "vfs.diskShareAccessMask").split(" ")) {
            assertFalse(config.getVfsSchemeProperties().containsKey(key), key);
            assertEquals("", fields.remove(key), key);
        }
        assertEquals("true", fields.remove("coordination"));
        assertEquals("true", fields.remove("sequential"));
        for (String key : new String[] {"sequence", "interval", "cronExpression"}) {
            assertEquals("", fields.remove(key), key);
        }
        for (String ui : new String[] {"class", "name", "generateSequences", "suspend", "onError", "scheduleType",
                "fileThrottlingType"}) {
            assertNotNull(fields.remove(ui), ui);
        }
        assertTrue(fields.isEmpty(), fields.toString());
    }

    @Test
    void theDeprecatedLockReleaseIntervalIsNotRequired() throws Exception {
        try (InputStreamReader reader = new InputStreamReader(
                getClass().getResourceAsStream("/uischema.json"), StandardCharsets.UTF_8)) {
            for (JsonElement group : JsonParser.parseReader(reader).getAsJsonObject().getAsJsonArray("elements")) {
                for (JsonElement entry : group.getAsJsonObject().getAsJsonObject("value").getAsJsonArray("elements")) {
                    JsonObject value = entry.getAsJsonObject().getAsJsonObject("value");
                    if (value.get("name").getAsString().equals("transport.vfs.AutoLockReleaseInterval")) {
                        assertEquals("false", value.get("required").getAsString());
                        return;
                    }
                }
            }
        }
        fail("The schema has no transport.vfs.AutoLockReleaseInterval.");
    }
}
