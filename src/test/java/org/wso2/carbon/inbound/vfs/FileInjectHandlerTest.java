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

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.AXIOMUtil;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.axis2.Constants;
import org.apache.axis2.AxisFault;
import org.apache.axis2.builder.ApplicationXMLBuilder;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.axis2.format.PlainTextBuilder;
import org.apache.axis2.format.PlainTextFormatter;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.transport.http.ApplicationXMLFormatter;
import org.apache.axis2.transport.http.SOAPMessageFormatter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.commons.json.JsonStreamBuilder;
import org.apache.synapse.config.xml.MediatorFactoryFinder;
import org.apache.synapse.config.xml.SequenceMediatorFactory;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.mediators.eip.splitter.CloneMediator;
import org.apache.synapse.mediators.eip.splitter.IterateMediator;
import org.apache.synapse.mediators.v2.ForEachMediatorV2;
import org.apache.synapse.mediators.v2.ScatterGather;
import org.apache.synapse.util.xpath.SynapseXPath;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;
import org.mockito.ArgumentCaptor;
import org.wso2.org.apache.commons.vfs2.FileContent;
import org.wso2.org.apache.commons.vfs2.FileName;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.VFS;
import org.wso2.vfs.shared.failstate.DigestingInputStream;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The content of one file on its way into mediation.
 */
class FileInjectHandlerTest {

    // The SHA-256 of zero bytes.
    private static final String EMPTY_DIGEST = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static final String ENDPOINT = "inject";

    @TempDir
    Path folder;

    // An empty file builds an empty envelope and goes into mediation once. The content is read through the real
    // digest and record streams, from a provider stream that closes itself at the end of the file, as the SFTP stream
    // of the provider does. A SOAP type in upper case, a type with no builder in any case, and no type with an extension
    // other than xml and txt reach the SOAP builder as a type that it does not take as SOAP or REST. It then gives no
    // element for an empty file, and the transport makes an empty envelope of that.
    @ParameterizedTest
    @CsvSource({"application/xml,e.xml,false,false", "application/xml,e.xml,false,true",
            "application/xml,e.xml,true,false", "application/xml,e.xml,true,true",
            "APPLICATION/SOAP+XML,e.txt,false,false", "APPLICATION/SOAP+XML,e.txt,false,true",
            "APPLICATION/SOAP+XML,e.txt,true,false", "APPLICATION/SOAP+XML,e.txt,true,true",
            "TEXT/XML,e.txt,false,false", "TEXT/XML,e.txt,true,false",
            "application/soap+xml,e.txt,false,false", "application/soap+xml,e.txt,true,false",
            "application/soap+xml; charset=UTF-8,e.txt,true,false", "text/xml,e.txt,true,false",
            "text/xml; charset=UTF-8,e.txt,true,false",
            "application/x-unknown,e.txt,false,false", "application/x-unknown,e.txt,false,true",
            "application/x-unknown,e.txt,true,false", "application/x-unknown,e.txt,true,true",
            "APPLICATION/X-UNKNOWN,e.txt,true,false",
            "'',e.dat,false,false", "'',e.dat,false,true", "'',e.dat,true,false", "'',e.dat,true,true",
            "'',e.xml,false,false", "'',e.xml,true,false"})
    void emptyFileIsMediatedOnceWithAnEmptyBody(String type, String name, boolean build, boolean streaming)
            throws Exception {
        Path empty = Files.createFile(folder.resolve(name));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        if (!type.isEmpty()) {
            properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, type);
        }
        properties.setProperty(VFSConstants.TRANSPORT_BUILD, String.valueOf(build));
        properties.setProperty(VFSConstants.STREAMING, String.valueOf(streaming));

        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        List<SOAPEnvelope> injected = new ArrayList<>();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            injected.add(((Axis2MessageContext) call.getArgument(0)).getEnvelope());
            return true;
        });

        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject file = VFS.getManager().resolveFile(empty.toUri().toString());
        FileIdentity identity = new FileIdentity(file.getName().getURI(), 0,
                Files.getLastModifiedTime(empty).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content content = new FileInjectHandler.Content(file, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), content));
            assertNull(content.readFailure());
        } finally {
            file.close();
        }
        assertEquals(1, injected.size());
        assertNull(injected.get(0).getBody().getFirstElement());
        assertEquals(EMPTY_DIGEST, identity.digest());
    }

    // A file that the builder refuses has its own ERROR line, which names the file with the password masked. Two
    // files refused with the same cause give two lines. The same file again in the window of the repeat limiter gives
    // no new line.
    @Test
    void aRefusedFileIsNamedInItsOwnLine() throws Exception {
        Log out = mock(Log.class);
        when(out.isErrorEnabled()).thenReturn(true);
        NamedLog log = NamedLog.inbound(out, ENDPOINT, new RepeatLimiter());
        for (String name : new String[]{"a.xml", "b.xml", "a.xml"}) {
            assertFalse(refuse(name, log));
        }
        ArgumentCaptor<Object> lines = ArgumentCaptor.forClass(Object.class);
        verify(out, times(2)).error(lines.capture());
        String first = lines.getAllValues().get(0).toString();
        String second = lines.getAllValues().get(1).toString();
        String line = "[inbound:" + ENDPOINT + "] File mediation failed for sftp://user:***@host/in/";
        assertTrue(first.startsWith(line + "a.xml."), first);
        assertTrue(second.startsWith(line + "b.xml."), second);
        assertFalse(first.contains("secret"), first);
        assertFalse(second.contains("secret"), second);
    }

    // A file that fails on every poll gives one line in the window of the repeat limiter. The first line after the
    // window carries the count of the lines held back.
    @Test
    @Tag("testbuild")
    void aFileThatFailsOnEveryPollIsNamedOncePerWindow() throws Exception {
        String previous = System.setProperty("vfs.testpoints.dir", folder.toString());
        try {
            Log out = mock(Log.class);
            when(out.isErrorEnabled()).thenReturn(true);
            NamedLog log = NamedLog.inbound(out, ENDPOINT, new RepeatLimiter());
            for (int poll = 0; poll < 4; poll++) {
                if (poll == 3) {
                    try (Stream<Path> children = Files.list(folder)) {
                        Path copy = children.filter(Files::isDirectory).findFirst().get();
                        Files.write(copy.resolve("clock.repeat.mono"), "300010".getBytes(StandardCharsets.UTF_8));
                    }
                }
                assertFalse(refuse("a.xml", log));
            }
            ArgumentCaptor<Object> lines = ArgumentCaptor.forClass(Object.class);
            verify(out, times(2)).error(lines.capture());
            String last = lines.getAllValues().get(1).toString();
            assertTrue(last.contains("File mediation failed for sftp://user:***@host/in/a.xml."), last);
            assertTrue(last.endsWith(" (skipped 2)"), last);
        } finally {
            if (previous == null) {
                System.clearProperty("vfs.testpoints.dir");
            } else {
                System.setProperty("vfs.testpoints.dir", previous);
            }
        }
    }

    // Mediates one file of an SFTP folder whose content is not XML, so the XML builder refuses it.
    private boolean refuse(String name, NamedLog log) throws Exception {
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, "sftp://user:secret@host/in");
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, "application/xml");
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        FileName fileName = VFS.getManager().resolveURI("sftp://user:secret@host/in/" + name);
        FileObject file = mock(FileObject.class);
        FileContent content = mock(FileContent.class);
        when(file.getName()).thenReturn(fileName);
        when(file.getContent()).thenReturn(content);
        when(content.getInputStream()).thenAnswer(call ->
                new ByteArrayInputStream("not xml".getBytes(StandardCharsets.UTF_8)));
        FileIdentity identity = new FileIdentity(fileName.getURI(), 7, 0, FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(file, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            boolean result = handler.invoke(sequence, ENDPOINT, new HashMap<>(), files);
            assertNull(files.readFailure());
            return result;
        }
    }

    // A type with a charset, as the transport listener reads it. The charset is on the axis2 context, where the
    // builder and the formatter read it, so the file is decoded with it and written in it again. A type with no
    // charset leaves it unset, and the content is UTF-8.
    @ParameterizedTest
    @CsvSource({"text/plain,ISO-8859-1,false", "text/plain,ISO-8859-1,true", "text/plain,UTF-16,false",
            "text/plain,UTF-16,true", "application/xml,ISO-8859-1,false", "application/xml,ISO-8859-1,true",
            "application/xml,UTF-16,false", "application/xml,UTF-16,true", "text/xml,ISO-8859-1,false",
            "text/xml,ISO-8859-1,true", "text/xml,UTF-16,false", "text/xml,UTF-16,true", "text/plain,'',false",
            "text/plain,'',true", "application/xml,'',true", "text/xml,'',true"})
    void charsetOfTheTypeIsOnTheAxis2Context(String type, String charset, boolean build) throws Exception {
        Charset encoding = charset.isEmpty() ? StandardCharsets.UTF_8 : Charset.forName(charset);
        String text = "caf\u00e9 \u00fcber";
        String envelope = "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\"><s:Body><a>" + text
                + "</a></s:Body></s:Envelope>";
        String content = "text/plain".equals(type) ? text : "text/xml".equals(type) ? envelope : "<a>" + text + "</a>";
        Path file = Files.write(folder.resolve("c.dat"), content.getBytes(encoding));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE,
                charset.isEmpty() ? type : type + "; charset=" + charset);
        properties.setProperty(VFSConstants.TRANSPORT_BUILD, String.valueOf(build));
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        axis.addMessageBuilder("text/plain", new PlainTextBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        MessageFormatter formatter = "text/plain".equals(type) ? new PlainTextFormatter()
                : "text/xml".equals(type) ? new SOAPMessageFormatter() : new ApplicationXMLFormatter();
        List<Object> property = new ArrayList<>();
        ByteArrayOutputStream written = new ByteArrayOutputStream();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            MessageContext message = ((Axis2MessageContext) call.getArgument(0)).getAxis2MessageContext();
            property.add(message.getProperty(Constants.Configuration.CHARACTER_SET_ENCODING));
            formatter.writeTo(message, BaseUtils.getOMOutputFormat(message), written, false);
            return true;
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject source = VFS.getManager().resolveFile(file.toUri().toString());
        FileIdentity identity = new FileIdentity(source.getName().getURI(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(source, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), files));
        } finally {
            source.close();
        }
        assertEquals(1, property.size());
        assertEquals(charset.isEmpty() ? null : charset, property.get(0));
        String declaration = "text/xml".equals(type)
                ? "<?xml version='1.0' encoding='" + (charset.isEmpty() ? "UTF-8" : charset) + "'?>" : "";
        assertArrayEquals((declaration + content).getBytes(encoding), written.toByteArray(),
                new String(written.toByteArray(), encoding));
    }

    // A new message of Synapse has a SOAP 1.2 envelope, as Axis2SynapseEnvironment gives it. The file is built as on
    // the transport, on a SOAP 1.1 message. So the relay builder of the pack, which asks the message for its version,
    // builds SOAP 1.1 under application/octet-stream and application/pdf. A SOAP file keeps its own version.
    @ParameterizedTest
    @CsvSource({"application/octet-stream,bytes,http://schemas.xmlsoap.org/soap/envelope/",
            "application/pdf,bytes,http://schemas.xmlsoap.org/soap/envelope/",
            "application/soap+xml,http://www.w3.org/2003/05/soap-envelope,http://www.w3.org/2003/05/soap-envelope",
            "text/xml,http://schemas.xmlsoap.org/soap/envelope/,http://schemas.xmlsoap.org/soap/envelope/"})
    void fileIsBuiltOnASoap11Message(String type, String content, String namespace) throws Exception {
        String text = content.startsWith("http") ? "<s:Envelope xmlns:s=\"" + content + "\"><s:Body><a/></s:Body>"
                + "</s:Envelope>" : "some bytes";
        Path file = Files.write(folder.resolve("f.dat"), text.getBytes(StandardCharsets.UTF_8));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, type);
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/octet-stream", new RelayLikeBuilder());
        axis.addMessageBuilder("application/pdf", new RelayLikeBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            message.setEnvelope(OMAbstractFactory.getSOAP12Factory().getDefaultEnvelope());
            return new Axis2MessageContext(message, configuration, environment);
        });
        List<SOAPEnvelope> injected = new ArrayList<>();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            injected.add(((Axis2MessageContext) call.getArgument(0)).getEnvelope());
            return true;
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileObject source = VFS.getManager().resolveFile(file.toUri().toString());
        FileIdentity identity = new FileIdentity(source.getName().getURI(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(source, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            assertTrue(handler.invoke(sequence, ENDPOINT, new HashMap<>(), files));
        } finally {
            source.close();
        }
        assertEquals(1, injected.size());
        assertEquals(namespace, injected.get(0).getNamespace().getNamespaceURI());
    }

    // A builder that marks the content, reads its start and resets it, then reads the content to its end. It gets the
    // whole file after the reset, and the hash in the identity is the hash of the whole file.
    @Test
    void aBuilderThatMarksAndResetsGetsTheWholeFileAndItsHash() throws Exception {
        byte[] payload = new byte[20000];
        new Random(5).nextBytes(payload);
        Path path = Files.write(folder.resolve("m.dat"), payload);
        FileObject file = VFS.getManager().resolveFile(path.toUri().toString());
        FileIdentity identity = new FileIdentity(file.getName().getURI(), payload.length,
                Files.getLastModifiedTime(path).toMillis(), FileIdentity.TimeSource.LISTING, null);
        StepBuilder builder = new StepBuilder(100);
        try {
            assertTrue(read(file, identity, mock(Generation.class), builder), String.valueOf(builder.failure));
        } finally {
            file.close();
        }
        assertArrayEquals(payload, builder.content.toByteArray());
        StringBuilder hash = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(payload)) {
            hash.append(String.format("%02x", value));
        }
        assertEquals(hash.toString(), identity.digest());
    }

    // The relay builder of the pack reads the content in steps of 1 KiB. The steps come from a buffer of 8 KiB above
    // the record and digest streams. So the provider stream, and every stream above it up to the buffer, sees one read
    // of 8 KiB for each 8 KiB of the file and one more read at its end. The progress of the read is marked once for
    // each 8 KiB.
    @Test
    void stepsOf1KiBReachTheProviderStreamAsReadsOf8KiB() throws Exception {
        byte[] payload = new byte[64 * 1024];
        new Random(7).nextBytes(payload);
        List<Integer> reads = new ArrayList<>();
        FileName name = VFS.getManager().resolveURI("sftp://user:secret@host/in/s.dat");
        FileObject file = mock(FileObject.class);
        FileContent content = mock(FileContent.class);
        when(file.getName()).thenReturn(name);
        when(file.getContent()).thenReturn(content);
        when(content.getInputStream()).thenReturn(new ByteArrayInputStream(payload) {
            @Override
            public synchronized int read() {
                reads.add(1);
                return super.read();
            }

            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                reads.add(length);
                return super.read(bytes, offset, length);
            }
        });
        FileIdentity identity = new FileIdentity(name.getURI(), payload.length, 0, FileIdentity.TimeSource.LISTING,
                null);
        Generation generation = mock(Generation.class);
        StepBuilder builder = new StepBuilder(0);
        assertTrue(read(file, identity, generation, builder), String.valueOf(builder.failure));
        assertArrayEquals(payload, builder.content.toByteArray());
        assertEquals(payload.length / 8192 + 1, reads.size());
        assertEquals(Collections.singleton(8192), new HashSet<>(reads));
        verify(generation, times(payload.length / 8192)).progress(anyLong());
        verify(generation, times(payload.length / 8192)).progress(8192);
    }

    // Mediates one file under application/octet-stream with the given builder. The content is read through the real
    // digest and record streams.
    private boolean read(FileObject file, FileIdentity identity, Generation generation, Builder builder)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, "application/octet-stream");
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        SequenceMediator sequence = mock(SequenceMediator.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        when(sequence.isInitialized()).thenReturn(true);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/octet-stream", builder);
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenReturn(true);
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        NamedLog log = NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter());
        FileInjectHandler handler = new FileInjectHandler("process", null, environment, new VFSConfig(properties));
        try (FileInjectHandler.Content content = new FileInjectHandler.Content(file, identity, store, generation,
                mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            boolean result = handler.invoke(sequence, ENDPOINT, new HashMap<>(), content);
            assertNull(content.readFailure());
            return result;
        }
    }

    // A fault that the error sequence handles leaves the file processed, as on the transport listener. The file fails
    // only when the flow leaves an error code in the transport headers. An empty text there is no error code, and a
    // value that is not a text fails the file, as it failed the file on the transport with an exception. The flow and
    // the error sequence run in the engine: a connector operation puts its own code on the message and throws, and
    // for any other fault the fault handler puts the code 0.
    @ParameterizedTest
    @CsvSource({"700101,,true", "700101,LINE,false", ",,true", ",LINE,false", "700101,EMPTY,true",
            "700101,NUMBER,false"})
    void aHandledFaultFailsTheFileOnlyByAnErrorCodeInTheTransportHeaders(String code, String line, boolean processed)
            throws Exception {
        String value = "LINE".equals(line) ? "expression=\"get-property('ERROR_CODE')\"/>"
                : "EMPTY".equals(line) ? "value=\"\"/>" : "value=\"0\" type=\"INTEGER\"/>";
        String transport = line == null ? "" : "<property name=\"ERROR_CODE\" scope=\"transport\" " + value;
        Engine run = mediate(new Properties(), errorSequence(transport), new FailingOperation(code));
        assertEquals(processed, run.processed);
        assertEquals("yes", run.message.getProperty("handled"));
        assertEquals(code == null ? "0" : code, String.valueOf(run.message.getProperty(SynapseConstants.ERROR_CODE)));
    }

    // An error sequence that marks the message as handled, with more mediators after that mark.
    private static String errorSequence(String more) {
        return sequence("error", "<property name=\"handled\" value=\"yes\"/>" + more);
    }

    private static String sequence(String name, String mediators) {
        return "<sequence xmlns=\"http://ws.apache.org/ns/synapse\" name=\"" + name + "\">" + mediators + "</sequence>";
    }

    // The outcome of one file mediated by the engine.
    private static final class Engine {
        boolean processed;
        String file;
        org.apache.synapse.MessageContext message;
    }

    private Engine mediate(Properties extra, String error, AbstractMediator... flow) throws Exception {
        Map<String, String> sequences = new HashMap<>();
        sequences.put("error", error);
        return mediate(extra, "application/xml", "<a/>", sequences, NamedLog.inbound(
                LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter()), flow);
    }

    // Mediates one file through a sequence of the given mediators, with the error sequence "error" and the other named
    // sequences of the given XML. The inject runs in the engine of synapse, so the sequence, its fault handler and the
    // error sequences are the real ones. A type of null configures no content type.
    private Engine mediate(Properties extra, String type, String body, Map<String, String> sequences, NamedLog log,
                           AbstractMediator... flow) throws Exception {
        Path file = Files.write(folder.resolve("in" + System.nanoTime() + (type == null ? ".dat" : ".xml")),
                body.getBytes(StandardCharsets.UTF_8));
        Properties properties = new Properties();
        properties.setProperty(VFSConstants.TRANSPORT_FILE_FILE_URI, folder.toUri().toString());
        if (type != null) {
            properties.setProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE, type);
        }
        properties.putAll(extra);
        SynapseConfiguration configuration = mock(SynapseConfiguration.class);
        when(configuration.getProperty(anyString(), anyString())).thenAnswer(call -> call.getArgument(1));
        SynapseEnvironment environment = mock(SynapseEnvironment.class);
        when(environment.getSynapseConfiguration()).thenReturn(configuration);
        InboundEndpoint inbound = mock(InboundEndpoint.class);
        when(inbound.getArtifactContainerName()).thenReturn(ENDPOINT);
        when(configuration.getInboundEndpoint(ENDPOINT)).thenReturn(inbound);
        SequenceMediator sequence = new SequenceMediator();
        sequence.setName("process");
        for (AbstractMediator mediator : flow) {
            sequence.addChild(mediator);
        }
        when(configuration.getSequence("process")).thenReturn(sequence);
        for (Map.Entry<String, String> named : sequences.entrySet()) {
            when(configuration.getSequence(named.getKey())).thenReturn(new SequenceMediatorFactory()
                    .createMediator(AXIOMUtil.stringToOM(named.getValue()), new Properties()));
        }
        AxisConfiguration axis = new AxisConfiguration();
        axis.addMessageBuilder("application/xml", new ApplicationXMLBuilder());
        axis.addMessageBuilder("application/json", new JsonStreamBuilder());
        ConfigurationContext context = new ConfigurationContext(axis);
        when(environment.createMessageContext()).thenAnswer(call -> {
            MessageContext message = new MessageContext();
            message.setConfigurationContext(context);
            return new Axis2MessageContext(message, configuration, environment);
        });
        Axis2SynapseEnvironment engine = new Axis2SynapseEnvironment(context, configuration);
        Engine run = new Engine();
        run.file = file.getFileName().toString();
        when(environment.injectInbound(any(), same(sequence), eq(true))).thenAnswer(call -> {
            run.message = call.getArgument(0);
            return engine.injectInbound(run.message, sequence, true);
        });
        FailStateStore store = mock(FailStateStore.class);
        when(store.digesting(any(), any(), any())).thenAnswer(call ->
                new DigestingInputStream(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        FileObject source = VFS.getManager().resolveFile(file.toUri().toString());
        FileIdentity identity = new FileIdentity(source.getName().getURI(), Files.size(file),
                Files.getLastModifiedTime(file).toMillis(), FileIdentity.TimeSource.LISTING, null);
        FileInjectHandler handler = new FileInjectHandler("process", "error", environment, new VFSConfig(properties));
        try (FileInjectHandler.Content files = new FileInjectHandler.Content(source, identity, store,
                mock(Generation.class), mock(VfsSession.class), ENDPOINT, mock(ReplySender.class), log)) {
            run.processed = handler.invoke(sequence, ENDPOINT, new HashMap<>(), files);
            assertNull(files.readFailure());
        } finally {
            source.close();
        }
        assertTrue(run.message != null, "The file reached the flow");
        return run;
    }

    // With FailOnErrorCode a file whose flow ends with an error code on the message fails, also when the error
    // sequence handled the fault, and the ERROR line names the file and the code. A flow that removes the code has
    // handled the error, and its file is processed. With the parameter off a handled fault leaves the file processed,
    // as before. The code of a connector operation and the code 0 of the fault handler of the engine count the same.
    @ParameterizedTest
    @CsvSource({"700101,true,LOG,false", "700101,true,REMOVE,true", "700101,false,LOG,true", ",true,LOG,false",
            ",true,REMOVE,true", ",false,LOG,true"})
    void failOnErrorCodeFailsAFileWhoseFlowEndsWithAnErrorCode(String code, boolean on, String error,
                                                                boolean processed) throws Exception {
        Log out = mock(Log.class);
        when(out.isErrorEnabled()).thenReturn(true);
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, String.valueOf(on));
        Map<String, String> sequences = new HashMap<>();
        sequences.put("error", errorSequence("REMOVE".equals(error)
                ? "<property name=\"ERROR_CODE\" action=\"remove\"/>" : "<log level=\"custom\"/>"));
        Engine run = mediate(extra, "application/xml", "<a/>", sequences,
                NamedLog.inbound(out, ENDPOINT, new RepeatLimiter()), new FailingOperation(code));
        assertEquals(processed, run.processed);
        assertEquals("yes", run.message.getProperty("handled"));
        ArgumentCaptor<Object> lines = ArgumentCaptor.forClass(Object.class);
        verify(out, times(processed ? 0 : 1)).error(lines.capture());
        if (!processed) {
            String line = lines.getValue().toString();
            assertTrue(line.startsWith("[inbound:" + ENDPOINT + "] File mediation failed for file://"), line);
            assertTrue(line.endsWith("/" + run.file + ". The flow ended with ERROR_CODE " + (code == null ? "0" : code)
                    + "."), line);
        }
    }

    // A flow that ends well leaves no error code, and its file is processed with FailOnErrorCode.
    @Test
    void failOnErrorCodeLeavesAFlowThatEndsWellProcessed() throws Exception {
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, "true");
        Engine run = mediate(extra, errorSequence(""), new Passing());
        assertTrue(run.processed);
        assertEquals("yes", run.message.getProperty("passed"));
        assertNull(run.message.getProperty("handled"));
    }

    // A sequence inside the flow has its own error sequence, which recovers, for example with a write to another
    // place, and the flow ends there with no other fault. The code of the first fault stays on the message, so with
    // FailOnErrorCode the file fails. A flow that recovers from an error removes ERROR_CODE.
    @ParameterizedTest
    @CsvSource({"false,false", "true,true"})
    void anInnerErrorSequenceThatRecoversFailsTheFileUnlessItRemovesTheCode(boolean removes, boolean processed)
            throws Exception {
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, "true");
        Map<String, String> sequences = new HashMap<>();
        sequences.put("error", errorSequence(""));
        sequences.put("recover", sequence("recover", "<property name=\"recovered\" value=\"yes\"/>"
                + (removes ? "<property name=\"ERROR_CODE\" action=\"remove\"/>" : "")));
        SequenceMediator inner = new SequenceMediator();
        inner.setErrorHandler("recover");
        inner.addChild(new FailingOperation("700101"));
        Engine run = mediate(extra, "application/xml", "<a/>", sequences, NamedLog.inbound(
                LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter()), inner);
        assertEquals(processed, run.processed);
        assertEquals("yes", run.message.getProperty("recovered"));
        assertNull(run.message.getProperty("handled"));
    }

    // A file with no content and no type reaches a messaging connector that refuses an empty message with its own
    // code, and the error sequence handles it. With FailOnErrorCode the file fails with the code of that connector.
    // Without it the file is processed, as before.
    @ParameterizedTest
    @CsvSource({"true,false", "false,true"})
    void anEmptyFileThatAnotherConnectorRefusesFailsWithFailOnErrorCode(boolean on, boolean processed)
            throws Exception {
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, String.valueOf(on));
        Map<String, String> sequences = new HashMap<>();
        sequences.put("error", errorSequence(""));
        Engine run = mediate(extra, null, "", sequences, NamedLog.inbound(
                LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter()),
                new FailingOperation("700124"));
        assertEquals(processed, run.processed);
        assertEquals("yes", run.message.getProperty("handled"));
        assertEquals("700124", run.message.getProperty(SynapseConstants.ERROR_CODE));
        assertNull(run.message.getEnvelope().getBody().getFirstElement());
    }

    // A fault in a branch of an iterate is on a copy of the message, and the branch handles it there. The message of
    // the file carries no error code, so the file is processed, also with FailOnErrorCode.
    @Test
    void aFaultInAnIterateBranchIsNotSeenByFailOnErrorCode() throws Exception {
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, "true");
        IterateMediator iterate = new IterateMediator();
        iterate.setExpression(new SynapseXPath("//a"));
        iterate.setContinueParent(true);
        iterate.setTarget(failingTarget());
        Engine run = mediate(extra, errorSequence(""), iterate, new Passing());
        assertTrue(run.processed);
        assertNull(run.message.getProperty(SynapseConstants.ERROR_CODE));
        assertEquals("yes", run.message.getProperty("passed"));
    }

    // A clone that runs its branches one after the other handles the fault of a branch on its copy, and then fails
    // itself on the message of the file, because it reads the error flag of the branch as a text and the branch sets
    // it as a boolean. So the error sequence of the flow runs with the code 0, and with FailOnErrorCode the file fails.
    @Test
    void aSequentialCloneFailsOnTheMessageOfTheFileAfterAFaultInABranch() throws Exception {
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, "true");
        CloneMediator clone = new CloneMediator();
        clone.setSequential(true);
        clone.setContinueParent(true);
        clone.addTarget(failingTarget());
        Engine run = mediate(extra, errorSequence(""), clone, new Passing());
        assertFalse(run.processed);
        assertEquals("0", String.valueOf(run.message.getProperty(SynapseConstants.ERROR_CODE)));
        assertEquals("yes", run.message.getProperty("handled"));
        assertNull(run.message.getProperty("passed"));
    }

    // A scatter gather and a foreach with a collection give each branch a copy of the message, and a branch handles its
    // fault there. The message of the file carries no error code, so the file is processed, also with FailOnErrorCode.
    // Both run their branches one after the other here, so the fault happens on the poll thread.
    @ParameterizedTest
    @CsvSource({"SCATTER_GATHER", "FOREACH"})
    void aFaultInABranchOfAScatterGatherOrAForeachIsNotSeen(String kind) throws Exception {
        boolean scatter = "SCATTER_GATHER".equals(kind);
        Properties extra = new Properties();
        extra.setProperty(VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE, "true");
        String xml = scatter
                ? "<scatter-gather xmlns=\"http://ws.apache.org/ns/synapse\" parallel-execution=\"false\" "
                + "target=\"body\" result-content-type=\"JSON\"><aggregation expression=\"${payload}\"/>"
                + "<sequence/><sequence/></scatter-gather>"
                : "<foreach xmlns=\"http://ws.apache.org/ns/synapse\" collection=\"${xpath('//i')}\" "
                + "parallel-execution=\"false\" update-original=\"true\"><sequence/></foreach>";
        AbstractMediator branches = (AbstractMediator) MediatorFactoryFinder.getInstance()
                .getMediator(AXIOMUtil.stringToOM(xml), new Properties());
        Target first = branches instanceof ScatterGather ? ((ScatterGather) branches).getTargets().get(0)
                : ((ForEachMediatorV2) branches).getTarget();
        first.getSequence().addChild(new FailingOperation("700101"));
        Map<String, String> sequences = new HashMap<>();
        sequences.put("error", errorSequence(""));
        Engine run = mediate(extra, scatter ? "application/json" : "application/xml",
                scatter ? "{\"items\":[{\"n\":1},{\"n\":2}]}" : "<a><i>1</i><i>2</i></a>", sequences,
                NamedLog.inbound(LogFactory.getLog(FileInjectHandlerTest.class), ENDPOINT, new RepeatLimiter()),
                branches, new Passing());
        String state = "code " + run.message.getProperty(SynapseConstants.ERROR_CODE) + ", message "
                + run.message.getProperty(SynapseConstants.ERROR_MESSAGE);
        assertTrue(run.processed, state);
        assertNull(run.message.getProperty(SynapseConstants.ERROR_CODE), state);
        assertNull(run.message.getProperty("handled"), state);
    }

    // A branch that runs on the poll thread and fails as a file connector operation fails.
    private static Target failingTarget() {
        SequenceMediator branch = new SequenceMediator();
        branch.addChild(new FailingOperation("700101"));
        Target target = new Target();
        target.setSequence(branch);
        target.setAsynchronous(false);
        return target;
    }

    // Ends well.
    private static final class Passing extends AbstractMediator {
        @Override
        public boolean mediate(org.apache.synapse.MessageContext message) {
            message.setProperty("passed", "yes");
            return true;
        }
    }

    // Fails as a file connector operation fails: it puts its error code on the message and throws. With no code it
    // fails as any other mediator does, and the fault handler of the engine puts the code.
    private static final class FailingOperation extends AbstractMediator {
        private final String code;

        FailingOperation(String code) {
            this.code = code;
        }

        @Override
        public boolean mediate(org.apache.synapse.MessageContext message) {
            if (code != null) {
                message.setProperty(SynapseConstants.ERROR_CODE, code);
            }
            throw new SynapseException("The operation failed");
        }
    }

    // Builds as the relay builder of the pack does (org.wso2.carbon.relay.BinaryRelayBuilder): the envelope takes the
    // SOAP version of the message, with the content in a binary element.
    private static final class RelayLikeBuilder implements Builder {
        @Override
        public OMElement processDocument(InputStream input, String type, MessageContext message) throws AxisFault {
            try {
                while (input.read() >= 0) {
                    // The content is read and dropped. The test looks at the envelope only.
                }
            } catch (IOException failure) {
                throw AxisFault.makeFault(failure);
            }
            SOAPFactory factory = message.isSOAP11() ? OMAbstractFactory.getSOAP11Factory()
                    : OMAbstractFactory.getSOAP12Factory();
            SOAPEnvelope envelope = factory.getDefaultEnvelope();
            envelope.getBody().addChild(factory.createOMElement("binary",
                    factory.createOMNamespace("http://ws.apache.org/commons/ns/payload", "ns")));
            return envelope;
        }
    }

    // Reads the content in steps of 1 KiB and keeps it, as the relay builder of the pack does when the body is kept
    // (BinaryRelayBuilder.readAllFromInputSteam). With a peek it first marks the content, reads that many bytes one by
    // one and resets the content to its start, as a builder that looks at the start of the content does.
    private static final class StepBuilder implements Builder {
        private final int peek;
        private final ByteArrayOutputStream content = new ByteArrayOutputStream();
        private IOException failure;

        StepBuilder(int peek) {
            this.peek = peek;
        }

        @Override
        public OMElement processDocument(InputStream input, String type, MessageContext message) throws AxisFault {
            try {
                if (peek > 0) {
                    if (!input.markSupported()) {
                        throw new IOException("The content stream does not support mark.");
                    }
                    input.mark(peek);
                    for (int i = 0; i < peek; i++) {
                        input.read();
                    }
                    input.reset();
                }
                byte[] step = new byte[1024];
                int size;
                while ((size = input.read(step)) != -1) {
                    content.write(step, 0, size);
                }
            } catch (IOException error) {
                failure = error;
                throw AxisFault.makeFault(error);
            }
            return OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope();
        }
    }
}
