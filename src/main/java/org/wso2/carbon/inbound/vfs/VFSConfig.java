/*
*  Licensed to the Apache Software Foundation (ASF) under one
*  or more contributor license agreements.  See the NOTICE file
*  distributed with this work for additional information
*  regarding copyright ownership.  The ASF licenses this file
*  to you under the Apache License, Version 2.0 (the
*  "License"); you may not use this file except in compliance
*  with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing,
*  software distributed under the License is distributed on an
*   * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
*  KIND, either express or implied.  See the License for the
*  specific language governing permissions and limitations
*  under the License.
*/
package org.wso2.carbon.inbound.vfs;

import org.apache.axis2.AxisFault;
import org.apache.axis2.addressing.EndpointReference;
import org.apache.axis2.description.AxisService;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.axis2.transport.base.ParamUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.synapse.commons.crypto.CryptoUtil;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Holds information about an entry in the VFS transport poll table used by the
 * VFS Transport Listener
 */
public class VFSConfig {

    // operation after scan
    public static final int DELETE = 0;
    public static final int MOVE   = 1;
    public static final int NONE   = 2;

    /** File or Directory to scan */
    private String fileURI;
    /** The URI to send replies to. May be null. */
    private String replyFileURI;
    /** file name pattern for a directory or compressed file entry */
    private String fileNamePattern;
    /** Content-Type to use for the message */
    private String contentType;

    /** Should the last updated time timestamp be updated */
    private boolean updateLastModified;

    /** action to take after a successful poll */
    private int actionAfterProcess = DELETE;
    /** action to take after a poll with errors */
    private int actionAfterErrors = DELETE;
    /** action to take after a failed poll */
    private int actionAfterFailure = DELETE;

    /** where to move the file after processing */
    private String moveAfterProcess;
    /** where to move the file after encountering some errors */
    private String moveAfterErrors;
    /** where to move the file after total failure */
    private String moveAfterFailure;
    /** moved file will have this formatted timestamp prefix */    
    private DateFormat moveTimestampFormat;

    /** containing the time in [ms] between the size check on files (to avoid reading files which are currently written) */
    private String checkSizeInterval = null;
    /** does the checkSize Lock mechanisme take empty files or not, default = false */
    private String checkSizeIgnoreEmpty = "false";


    private boolean streaming;

    private int maxRetryCount;
    private long reconnectTimeout;
    private boolean fileLocking;

    private CryptoUtil cryptoUtil;
    private Properties secureVaultProperties;

    /**
     * Only files smaller than this limit will get processed, it can be configured with param
     * "transport.vfs.FileSizeLimit", and this will have default value -1 which means unlimited file size
     * This should be specified in bytes
     */
    private double fileSizeLimit = VFSConstants.DEFAULT_TRANSPORT_FILE_SIZE_LIMIT;

    private String moveAfterMoveFailure;

    private int nextRetryDurationForFailedMove;

    private String failedRecordFileName;

    private String failedRecordFileDestination;

    private String failedRecordTimestampFormat;

    private Integer fileProcessingInterval;
    
    private Integer fileProcessingCount;

    private Map<String, String> vfsSchemeProperties;
    private boolean autoLockRelease;

    private Long autoLockReleaseInterval;

    private Boolean autoLockReleaseSameNode;   
    
    private boolean distributedLock;
    
    private String fileSortParam;
    
    private boolean fileSortAscending;
    
    private boolean forceCreateFolder;
    
    private String subfolderTimestamp;
    
    private Long distributedLockTimeout;

    private volatile boolean canceled;

    private boolean clusterAware;

    /**
     * This parameter is used decide whether the resolving hostname IP of URIs are done at deployment or dynamically.
     * At usage default id 'false' which lead hostname resolution at deployment
     */
    private boolean resolveHostsDynamically = false;
    private boolean fileNotFoundLogged = false;

    private ParameterInclude params;

    private static final Log log = LogFactory.getLog(VFSConfig.class);
    
    private Long minimumAge = null; //defines a minimum age of a file before being consumed. Use to avoid just written files to be consumed
    private Long maximumAge = null; //defines a maximum age of a file being consumed. Old files will stay in the directory


    public VFSConfig(Properties properties) {

        // TODO: set the properties
        this.fileURI = properties.getProperty(VFSConstants.TRANSPORT_FILE_FILE_URI);
    }

    public String getFileURI() {
        if (resolveHostsDynamically) {
            try {
                return VFSUtils.resolveUriHost(fileURI);
            } catch (UnknownHostException | FileSystemException e) {
                String message = "Unable to resolve the hostname of transport.vfs.FileURI : " +
                        VFSUtils.maskURLPassword(fileURI);
                VFSTransportErrorHandler.logException(log, VFSTransportErrorHandler.LogType.WARN, message, e);
            }
        }
        return  fileURI;
    }

    public boolean isClusterAware() {
        return clusterAware;
    }

    public Map<String, String> getVfsSchemeProperties() {
        return vfsSchemeProperties;
    }

    private String resolveHostAtDeployment(String uri) throws AxisFault {
        if (!resolveHostsDynamically) {
            try {
                return VFSUtils.resolveUriHost(uri, new StringBuilder());
            } catch (FileSystemException e) {
                String errorMsg = "Unable to decode the malformed URI : " + VFSUtils.maskURLPassword(uri);
                //log the error since if we only throw AxisFault, we won't get the entire stacktrace in logs to
                // identify root cause to users
                VFSTransportErrorHandler.handleException(log, errorMsg, e);

            } catch (UnknownHostException e) {
                String errorMsg = "Error occurred while resolving hostname of URI : " + VFSUtils.maskURLPassword(uri);
                //log the error since if we only throw AxisFault, we won't get the entire stacktrace in logs to
                // identify root cause to users
                VFSTransportErrorHandler.handleException(log, errorMsg, e);
            }
        }
        return uri;
    }

    /**
     * Iterate ParameterInclude and decrypt parameters if required.
     *
     * @param params ParameterInclude instance
     * @throws AxisFault
     */
    private void decryptParamsIfRequired(ParameterInclude params) throws AxisFault {
        for (Parameter param : params.getParameters()) {
            if (param != null && param.getValue() != null && param.getValue() instanceof String) {
                param.setValue(decryptIfRequired(param.getValue().toString()));
            }
        }
    }

    /**
     * Helper method to decrypt parameters if required.
     * If the parameter is defined as - {wso2:vault-decrypt('Parameter')}, then this method will treat it as deryption
     * required and do the relevant decryption for that part.
     *
     * @param parameter
     * @return parameter
     * @throws AxisFault
     */
    private String decryptIfRequired(String parameter) throws AxisFault {
        if (parameter != null && !parameter.isEmpty()) {
            // Create a Pattern object
            Pattern r = Pattern.compile("\\{wso2:vault-decrypt\\('(.*?)'\\)\\}");

            // Now create matcher object.
            Matcher m = r.matcher(parameter);
            if (m.find()) {
                if (cryptoUtil == null) {
                    cryptoUtil = new CryptoUtil(secureVaultProperties);
                }
                if (!cryptoUtil.isInitialized()) {
                    VFSTransportErrorHandler.handleException(log, "Error initialising cryptoutil");
                }
                String toDecrypt = m.group(1);
                toDecrypt = new String(cryptoUtil.decrypt(toDecrypt.getBytes()));
                parameter = m.replaceFirst(toDecrypt);
            }
        }
        return parameter;
    }
}
