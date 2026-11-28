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

package org.wso2.carbon.inbound.vfs;

import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportInDescription;
import org.apache.synapse.commons.crypto.CryptoUtil;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Cipher;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PublicKey;
import java.util.Base64;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VFSVaultTest {
    private static final String FILE = "transport.vfs.FileURI";
    private static final String CONTENT = "transport.vfs.ContentType";
    private static final String OAEP = "RSA/ECB/OAEPwithSHA1andMGF1Padding";
    private static final String LEGACY = "RSA/ECB/PKCS1Padding";
    private static final String FIXTURE_PASSWORD = "temporary-vault-password";

    private static PublicKey publicKey;
    private static Path store;

    @BeforeAll
    static void makeDisposableKey(@TempDir Path folder) throws Exception {
        store = folder.resolve("fixture.jks");
        Path keytool = Paths.get(System.getProperty("java.home"), "bin", "keytool");
        Process process = new ProcessBuilder(keytool.toString(), "-genkeypair", "-storetype", "JKS",
                "-keystore", store.toString(), "-alias", "test", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "1", "-dname", "CN=fixture", "-storepass", FIXTURE_PASSWORD,
                "-keypass", FIXTURE_PASSWORD).redirectErrorStream(true)
                .redirectOutput(folder.resolve("keytool.log").toFile()).start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue());
        KeyStore keys = KeyStore.getInstance("JKS");
        try (InputStream stream = Files.newInputStream(store)) {
            keys.load(stream, FIXTURE_PASSWORD.toCharArray());
        }
        publicKey = keys.getCertificate("test").getPublicKey();
    }

    private static Properties receiver(String algorithm) {
        Properties p = new Properties();
        p.setProperty("keystore.identity.location", store.toString());
        p.setProperty("keystore.identity.store.password", FIXTURE_PASSWORD);
        p.setProperty("keystore.identity.key.password", FIXTURE_PASSWORD);
        p.setProperty("keystore.identity.type", "JKS");
        p.setProperty("keystore.identity.alias", "test");
        if (algorithm != null) {
            p.setProperty("cipher.algorithm", algorithm);
        }
        return p;
    }

    private static Axis2SynapseEnvironment environment(Properties settings) throws Exception {
        TransportInDescription receiver = new TransportInDescription("vfs");
        for (String name : settings.stringPropertyNames()) {
            receiver.addParameter(new Parameter(name, settings.getProperty(name)));
        }
        Axis2SynapseEnvironment environment = mock(Axis2SynapseEnvironment.class, RETURNS_DEEP_STUBS);
        when(environment.getAxis2ConfigurationContext().getAxisConfiguration().getTransportIn("vfs"))
                .thenReturn(receiver);
        return environment;
    }

    private static String encrypt(String algorithm, String clear) throws Exception {
        Cipher cipher = Cipher.getInstance(algorithm);
        cipher.init(Cipher.ENCRYPT_MODE, publicKey);
        return Base64.getEncoder().encodeToString(cipher.doFinal(clear.getBytes(StandardCharsets.UTF_8)));
    }

    private static Properties endpoint(String ciphertext) {
        Properties p = new Properties();
        p.setProperty(FILE, "sftp://rig:{wso2:vault-decrypt('" + ciphertext + "')}@host/in");
        return p;
    }

    @Test
    void legacyCiphertextUsesTheReceiverDefault() throws Exception {
        String input = encrypt(LEGACY, "old-password");
        VFSConfig config = new VFSConfig(endpoint(input), environment(receiver(null)), true);
        assertEquals("sftp://rig:old-password@host/in", config.getFileURI());
    }

    @Test
    void receiverControlsCipherSelectionForEveryParameter() throws Exception {
        String input = encrypt(OAEP, "tool-password");
        Properties p = endpoint(input);
        p.setProperty("transport.vfs.MoveAfterProcess", "ftp://rig:{wso2:vault-decrypt('" + input + "')}@other/out");
        VFSConfig config = new VFSConfig(p, environment(receiver(OAEP)), true);
        assertEquals("sftp://rig:tool-password@host/in", config.getFileURI());
        assertEquals("ftp://rig:tool-password@other/out", config.getMoveAfterProcess());
    }

    @Test
    void badInputCannotConstructTheConsumer() throws Exception {
        String mismatch = encrypt(OAEP, "tool-password");
        for (String input : new String[] {mismatch, "QUJDRA=="}) {
            Axis2SynapseEnvironment environment = environment(receiver(null));
            Properties p = endpoint(input);
            p.setProperty("cipher.algorithm", OAEP);
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                    new VFSConsumer(p, "files", environment, 1000, "main", null, false, true));
            assertEquals("Cannot decrypt " + FILE + ". Check the vfs receiver keystore settings.", e.getMessage());
            assertFalse(e.getMessage().contains(input));
            assertNull(e.getCause());
        }
    }

    @Test
    void emptyPlaintextAndInvalidCiphertextHaveTheSameHelperResult() throws Exception {
        String empty = encrypt(OAEP, "");
        Properties p = receiver(OAEP);
        CryptoUtil helper = new CryptoUtil(p);
        byte[] decrypted = helper.decrypt(empty.getBytes(StandardCharsets.UTF_8));
        assertNotNull(decrypted);
        assertEquals(0, decrypted.length);
        assertArrayEquals(decrypted, helper.decrypt("QUJDRA==".getBytes(StandardCharsets.UTF_8)));
        Axis2SynapseEnvironment environment = environment(p);
        assertThrows(IllegalArgumentException.class, () -> new VFSConfig(endpoint(empty), environment, true));

        Properties literal = new Properties();
        literal.setProperty(FILE, "file:///tmp");
        literal.setProperty(CONTENT, "");
        assertEquals("", new VFSConfig(literal, environment, true).getContentType());
    }
}
