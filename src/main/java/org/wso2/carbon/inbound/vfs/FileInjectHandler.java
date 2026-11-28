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

package org.wso2.carbon.inbound.vfs;

import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.util.UUIDGenerator;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.format.DataSourceMessageBuilder;
import org.apache.axis2.format.ManagedDataSource;
import org.apache.axis2.format.ManagedDataSourceFactory;
import org.apache.axis2.transport.TransportUtils;
import org.wso2.carbon.inbound.vfs.io.RecordingInputStream;
import org.wso2.carbon.inbound.vfs.reply.ReplySender;
import org.wso2.vfs.shared.health.EndpointHealth;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.UriMask;
import org.apache.synapse.inbound.InboundEndpointConstants;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.commons.vfs.VFSConstants;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.inbound.InboundEndpoint;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.transport.customlogsetter.CustomLogSetter;
import org.wso2.carbon.inbound.vfs.conn.VfsSession;
import org.wso2.carbon.inbound.vfs.poll.Generation;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.util.TestPoints;
import org.wso2.vfs.shared.failstate.FileIdentity;

import javax.mail.internet.ContentType;
import javax.mail.internet.ParseException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.axis2.context.MessageContext.TRANSPORT_HEADERS;

public class FileInjectHandler {

    private final String injectingSeq;
    private final String onErrorSeq;
    private final VFSConfig vfsProperties;
    private final SynapseEnvironment synapseEnvironment;

    public FileInjectHandler(String injectingSeq, String onErrorSeq, SynapseEnvironment synapseEnvironment,
                             VFSConfig vfsProperties) {
        this.injectingSeq = injectingSeq;
        this.onErrorSeq = onErrorSeq;
        this.synapseEnvironment = synapseEnvironment;
        this.vfsProperties = vfsProperties;
    }

    public SequenceMediator sequence() {
        return (SequenceMediator) synapseEnvironment.getSynapseConfiguration().getSequence(injectingSeq);
    }

    public static final class Content implements AutoCloseable {
        private final FileObject file;
        private final FileIdentity identity;
        private final Generation generation;
        private final VfsSession session;
        private final String endpoint;
        private final ReplySender reply;
        private final NamedLog log;
        private final List<RecordingInputStream> opened = new ArrayList<>();
        private ManagedDataSource dataSource;
        private IOException openFailure;
        private long openMillis;

        public Content(FileObject file, FileIdentity identity, Generation generation, VfsSession session,
                       String endpoint, ReplySender reply, NamedLog log) {
            this.file = file;
            this.identity = identity;
            this.generation = generation;
            this.session = session;
            this.endpoint = endpoint;
            this.reply = reply;
            this.log = log;
        }

        InputStream open() throws IOException {
            long threshold = -1;
            if (TestPoints.enabled()) {
                String bytes = TestPoints.arguments("session.barrier", endpoint, "endpoint=" + endpoint,
                        "generation=" + generation.number).get("bytes");
                if (bytes != null) {
                    threshold = Long.parseLong(bytes);
                }
            }
            long start = System.nanoTime();
            try {
                if (generation.isRetired()) {
                    throw new IOException("The file generation has retired.");
                }
                InputStream input = file.getContent().getInputStream();
                RecordingInputStream stream = new RecordingInputStream(input, generation, threshold,
                        count -> session.contentBarrier(file.getFileSystem(), count));
                opened.add(stream);
                // A builder that reads in small steps takes them from this buffer, so the streams below read in
                // blocks of 8 KiB. The buffer also lets a builder mark and reset the content.
                return new BufferedInputStream(stream, 8192);
            } catch (IOException error) {
                openFailure = error;
                openMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                throw error;
            }
        }

        public IOException readFailure() {
            for (RecordingInputStream stream : opened) {
                if (stream.failure() != null) {
                    return stream.failure();
                }
            }
            return openFailure;
        }

        public long failureMillis() {
            for (RecordingInputStream stream : opened) {
                if (stream.failure() != null) {
                    return stream.failureMillis();
                }
            }
            return openMillis;
        }

        @Override
        public void close() {
            try {
                if (dataSource != null) {
                    dataSource.destroy();
                }
            } catch (RuntimeException failure) {
                log.warn("Could not close the data source. " + failure.getMessage());
            } finally {
                for (RecordingInputStream stream : opened) {
                    try {
                        stream.close();
                    } catch (IOException failure) {
                        log.warn("Could not close the file content. " + failure.getMessage());
                    }
                }
            }
        }
    }

    static void setStatisticsMetadata(org.apache.synapse.MessageContext message, String uri) {
        EndpointHealth.setStatisticsMetadata(uri, (key, fields) -> {
            message.setProperty(key, fields);
            Map<Object, Object> metadata = new HashMap<>();
            Object previous = message.getProperty(SynapseConstants.ANALYTICS_METADATA);
            if (previous instanceof Map) {
                metadata.putAll((Map<?, ?>) previous);
            }
            metadata.putAll((Map<?, ?>) fields);
            message.setProperty(SynapseConstants.ANALYTICS_METADATA, metadata);
        });
    }

    public boolean invoke(SequenceMediator seq, String name, Map<String, Object> headers, Content content) {
        NamedLog log = content.log;
        FileObject file = content.file;
        try {
            org.apache.synapse.MessageContext msgCtx = createMessageContext(headers);
            msgCtx.setProperty(SynapseConstants.INBOUND_ENDPOINT_NAME, name);
            msgCtx.setProperty(SynapseConstants.ARTIFACT_NAME, SynapseConstants.FAIL_SAFE_MODE_INBOUND_ENDPOINT + name);
            msgCtx.setProperty(SynapseConstants.IS_INBOUND, true);
            msgCtx.setProperty(InboundEndpointConstants.INBOUND_ENDPOINT_RESPONSE_WORKER, content.reply);
            setStatisticsMetadata(msgCtx, file.getName().getURI());

            InboundEndpoint inboundEndpoint = msgCtx.getConfiguration().getInboundEndpoint(name);
            CustomLogSetter.getInstance().setLogAppender(inboundEndpoint.getArtifactContainerName());
            MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx)
                    .getAxis2MessageContext();
            String contentType = vfsProperties.getContentType();
            if (contentType == null || contentType.trim().equals("")) {
                if (file.getName().getExtension().toLowerCase().endsWith("xml")) {
                    contentType = "text/xml";
                } else if (file.getName().getExtension().toLowerCase().endsWith("txt")) {
                    contentType = "text/plain";
                }
            } else {
                // Extract the charset encoding from the configured content type and
                // set the CHARACTER_SET_ENCODING property as e.g. SOAPBuilder relies on this.
                String charSetEnc = null;
                try {
                    charSetEnc = new ContentType(contentType).getParameter("charset");
                } catch (ParseException ex) {
                    // ignore
                }
                // The builder and the formatter read the charset from the axis2 context, where the transport listener
                // sets it. The Synapse context keeps it as well, for a flow that reads it there.
                axis2MsgCtx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charSetEnc);
                msgCtx.setProperty(Constants.Configuration.CHARACTER_SET_ENCODING, charSetEnc);
            }
            if (log.isDebugEnabled()) {
                log.debug("Processed file " + UriMask.friendly(file.getName()) + " with content type " + contentType);
            }
            // Determine the message builder to use
            Builder builder;
            if (contentType == null) {
                log.debug("No content type specified. Using SOAP builder.");
                builder = new SOAPBuilder();
            } else {
                int index = contentType.indexOf(';');
                String type = index > 0 ? contentType.substring(0, index) : contentType;
                builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
                if (builder == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("No message builder found for type '" + type + "'. Falling back to SOAP.");
                    }
                    builder = new SOAPBuilder();
                }
            }

            // set the message payload to the message context
            boolean isStreaming = vfsProperties.isStreaming();

            OMElement documentElement;
            if (builder instanceof DataSourceMessageBuilder && isStreaming) {
                content.dataSource = ManagedDataSourceFactory.create(new FileObjectDataSource(file, contentType) {
                    @Override
                    public long getSize() {
                        return content.identity.size;
                    }

                    @Override
                    public InputStream getInputStream() throws IOException {
                        return content.open();
                    }
                });
                documentElement = ((DataSourceMessageBuilder) builder)
                        .processDocument(content.dataSource, contentType, axis2MsgCtx);
            } else {
                documentElement = builder.processDocument(content.open(), contentType, axis2MsgCtx);
            }

            // The SOAP builder gives no element for an empty file of a type that it does not take as SOAP or REST.
            // The envelope is then empty, as on the transport.
            if (vfsProperties.isBuild() && documentElement != null) {
                documentElement.build();
            }
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement));

            if (content.generation.isRetired()) {
                return false;
            }
            if (!seq.isInitialized()) {
                seq.init(synapseEnvironment);
            }
            seq.setErrorHandler(onErrorSeq);
            if (vfsProperties.isAppend()) {
                axis2MsgCtx.setProperty("transport.vfs.Append", "true");
            }
            if (!synapseEnvironment.injectInbound(msgCtx, seq, true)) {
                return false;
            }
            Map<?, ?> result = (Map<?, ?>) axis2MsgCtx.getProperty(TRANSPORT_HEADERS);
            if (content.reply.failed() || failedBy(result == null ? null : result.get(VFSConstants.ERROR_CODE))) {
                return false;
            }
            // With FailOnErrorCode the error code that the connector or the fault handler put on the message fails the
            // file. A flow that handled the error removes it. The flow is seen as it gives back this thread. A fault in
            // a branch of a scatter gather, a foreach with a collection, an iterate or a parallel clone is on a copy of
            // the message and is not seen.
            Object code = msgCtx.getProperty(SynapseConstants.ERROR_CODE);
            if (vfsProperties.isFailOnErrorCode() && failedBy(code)) {
                log.error("File mediation failed for " + UriMask.friendly(file.getName()) + ". The flow ended with "
                        + "ERROR_CODE " + code + ".");
                return false;
            }
            return true;
        } catch (Exception failure) {
            if (content.readFailure() == null && !content.reply.failed()) {
                log.error(Utils.withTrace("File mediation failed for " + UriMask.friendly(file.getName()) + ".",
                        failure));
            }
            return false;
        }
    }

    // The transport listener counts a file as failed for an error code in the transport headers that is not an empty
    // text. A value that is not a text failed the file there with an exception.
    private static boolean failedBy(Object code) {
        return code != null && !"".equals(code);
    }

    private org.apache.synapse.MessageContext createMessageContext(Map<String, Object> transportHeaders)
            throws AxisFault {
        org.apache.synapse.MessageContext msgCtx = synapseEnvironment.createMessageContext();
        MessageContext axis2MsgCtx = ((org.apache.synapse.core.axis2.Axis2MessageContext) msgCtx)
                .getAxis2MessageContext();
        // Synapse gives a new message a SOAP 1.2 envelope. The transport listener builds on a new axis2 message, which
        // is SOAP 1.1, so a builder that asks the message for its version builds SOAP 1.1 there. The message is SOAP
        // 1.1 until the file is built, as on the transport, and a SOAP 1.2 file keeps its own version.
        axis2MsgCtx.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(UUIDGenerator.getUUID());
        axis2MsgCtx.setProperty(TRANSPORT_HEADERS, transportHeaders);
        msgCtx.setProperty(MessageContext.CLIENT_API_NON_BLOCKING, true);
        return msgCtx;
    }
}
