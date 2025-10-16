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

import org.apache.axis2.AxisFault;
import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.ParameterInclude;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.crypto.CryptoUtil;
import org.wso2.org.apache.commons.vfs2.FileSystemException;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wso2.org.apache.commons.vfs2.provider.UriParser.extractQueryParams;

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
    private String replayFileName;

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
    private boolean build;

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
    private boolean append = false;
    private boolean avoidPermissionCheck = false;
    private boolean passive = false;
    private Long failedRecordNextRetryDuration = 30000L;

    public VFSConfig(Properties properties) {
        // Basic URIs
        this.fileURI = properties.getProperty(VFSConstants.TRANSPORT_FILE_FILE_URI);
        this.replyFileURI = properties.getProperty(VFSConstants.REPLY_FILE_URI);
        this.replayFileName   = properties.getProperty(VFSConstants.REPLY_FILE_NAME);
        this.fileNamePattern = properties.getProperty(VFSConstants.TRANSPORT_FILE_FILE_NAME_PATTERN);
        this.contentType = properties.getProperty(VFSConstants.TRANSPORT_FILE_CONTENT_TYPE);

        // Boolean/string flags
        this.updateLastModified = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.UPDATE_LAST_MODIFIED, "false"));
        this.streaming = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.STREAMING, "false"));
        this.build = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_BUILD, "false"));
        this.fileLocking = VFSConstants.TRANSPORT_FILE_LOCKING_ENABLED.equalsIgnoreCase(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_LOCKING, VFSConstants.TRANSPORT_FILE_LOCKING_DISABLED));
        this.forceCreateFolder = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.FORCE_CREATE_FOLDER, "false"));
        this.resolveHostsDynamically = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_RESOLVEHOST_DYNAMICALLY, "false"));
        this.autoLockRelease = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE, "false"));
        this.autoLockReleaseSameNode = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE, "false"));
        this.distributedLock = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK, "false"));
        this.clusterAware = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.CLUSTER_AWARE, "false"));

        // Actions after processing
        this.actionAfterProcess = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_PROCESS));
        this.actionAfterErrors = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_ERRORS));
        this.actionAfterFailure = parseAction(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_ACTION_AFTER_FAILURE));

        // Move locations
        this.moveAfterProcess = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        this.moveAfterErrors = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_ERRORS);
        this.moveAfterFailure = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE);
        this.moveAfterMoveFailure = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE);

        // Timestamp formatting
        String tsFormat = properties.getProperty(VFSConstants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT);
        if (tsFormat != null) {
            this.moveTimestampFormat = new SimpleDateFormat(tsFormat);
        }
        this.failedRecordTimestampFormat =
                properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT,
                        VFSConstants.DEFAULT_TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT);

        // Check size mechanism
        this.checkSizeInterval = properties.getProperty(VFSConstants.TRANSPORT_CHECK_SIZE_INTERVAL);
        this.checkSizeIgnoreEmpty = properties.getProperty(VFSConstants.TRANSPORT_CHECK_SIZE_IGNORE_EMPTY, "false");

        // Retry / reconnect
        this.maxRetryCount = Integer.parseInt(
                properties.getProperty(VFSConstants.MAX_RETRY_COUNT,
                        String.valueOf(VFSConstants.DEFAULT_MAX_RETRY_COUNT)));
        this.reconnectTimeout = Long.parseLong(
                properties.getProperty(VFSConstants.RECONNECT_TIMEOUT,
                        String.valueOf(VFSConstants.DEFAULT_RECONNECT_TIMEOUT)));
        this.nextRetryDurationForFailedMove = Integer.parseInt(
                properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION,
                        String.valueOf(VFSConstants.DEFAULT_NEXT_RETRY_DURATION)));

        // Failed record handling
        this.failedRecordFileName = properties.getProperty(
                VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_NAME,
                VFSConstants.DEFAULT_FAILED_RECORDS_FILE_NAME);
        this.failedRecordFileDestination = properties.getProperty(
                VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_DESTINATION,
                VFSConstants.DEFAULT_FAILED_RECORDS_FILE_DESTINATION);

        // Processing interval/count
        String intervalStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_INTERVAL);
        if (intervalStr != null) {
            this.fileProcessingInterval = Integer.valueOf(intervalStr);
        }
        String countStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_COUNT);
        if (countStr != null) {
            this.fileProcessingCount = Integer.valueOf(countStr);
        }

        // Lock release configs
        String autoReleaseIntervalStr = properties.getProperty(VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL);
        if (autoReleaseIntervalStr != null) {
            this.autoLockReleaseInterval = Long.valueOf(autoReleaseIntervalStr);
        }

        String distLockTimeoutStr = properties.getProperty(VFSConstants.TRANSPORT_DISTRIBUTED_LOCK_TIMEOUT);
        if (distLockTimeoutStr != null) {
            this.distributedLockTimeout = Long.valueOf(distLockTimeoutStr);
        }

        // Sorting configs
        this.fileSortParam = properties.getProperty(VFSConstants.FILE_SORT_PARAM);
        this.fileSortAscending = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.FILE_SORT_ORDER, "true"));

        // File size limit
        this.fileSizeLimit = Double.parseDouble(
                properties.getProperty(VFSConstants.TRANSPORT_FILE_SIZE_LIMIT,
                        String.valueOf(VFSConstants.DEFAULT_TRANSPORT_FILE_SIZE_LIMIT)));

        // Subfolder timestamp
        this.subfolderTimestamp = properties.getProperty(VFSConstants.SUBFOLDER_TIMESTAMP);

        // Min/Max Age
        String minAgeStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_MINIMUM_AGE);
        if (minAgeStr != null) {
            this.minimumAge = Long.valueOf(minAgeStr);
        }
        String maxAgeStr = properties.getProperty(VFSConstants.TRANSPORT_FILE_MAXIMUM_AGE);
        if (maxAgeStr != null) {
            this.maximumAge = Long.valueOf(maxAgeStr);
        }

        // Secure vault related
        this.secureVaultProperties = properties;
        this.append = Boolean.parseBoolean(
                properties.getProperty(VFSConstants.APPEND, "false"));

        this.passive = Boolean.parseBoolean(
                properties.getProperty("vfs.passive", "false"));

        String retryDurationStr = properties.getProperty(VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION);
        if (StringUtils.isNotEmpty(retryDurationStr)) {
            try {
                this.failedRecordNextRetryDuration = Long.parseLong(retryDurationStr);
            } catch (NumberFormatException e) {
                this.failedRecordNextRetryDuration = Long.parseLong(
                        properties.getProperty("30000"));
                log.warn("Invalid failedRecordNextRetryDuration value: " + retryDurationStr +
                        ". Using default: " + failedRecordNextRetryDuration);
            }
        }

        try {
            this.vfsSchemeProperties = extractQueryParams(this.fileURI);
        } catch (FileSystemException e) {
            this.vfsSchemeProperties = new HashMap<>();
            if (log.isDebugEnabled()) {
                log.debug("Error extracting query params from URI: " + Utils.maskURLPassword(this.fileURI), e);
            }
        }
    }

    /**
     * Small helper to map actions to int (DELETE / MOVE etc.)
     */
    private int parseAction(String action) {
        if (action == null) {
            return DELETE; // default
        }
        if ("MOVE".equalsIgnoreCase(action)) {
            return MOVE;
        } else if ("NONE".equalsIgnoreCase(action)) {
            return NONE;
        }
        return DELETE;
    }


    public String getFileURI() {
        if (resolveHostsDynamically) {
            try {
                return Utils.resolveUriHost(fileURI);
            } catch (UnknownHostException | FileSystemException e) {
                String message = "Unable to resolve the hostname of transport.vfs.FileURI : " +
                        Utils.maskURLPassword(fileURI);
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
                return Utils.resolveUriHost(uri, new StringBuilder());
            } catch (FileSystemException e) {
                String errorMsg = "Unable to decode the malformed URI : " + Utils.maskURLPassword(uri);
                // log the error since if we only throw AxisFault, we won't get the entire stacktrace in logs to
                // identify root cause to users
                VFSTransportErrorHandler.handleException(log, errorMsg, e);

            } catch (UnknownHostException e) {
                String errorMsg = "Error occurred while resolving hostname of URI : " + Utils.maskURLPassword(uri);
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

    public String getReplyFileURI() {
        return replyFileURI;
    }

    public String getFileNamePattern() {
        return fileNamePattern;
    }

    public String getContentType() {
        return contentType;
    }

    public boolean isUpdateLastModified() {
        return updateLastModified;
    }

    public int getActionAfterProcess() {
        return actionAfterProcess;
    }

    public int getActionAfterErrors() {
        return actionAfterErrors;
    }

    public int getActionAfterFailure() {
        return actionAfterFailure;
    }

    public String getMoveAfterProcess() {
        return moveAfterProcess;
    }

    public String getMoveAfterErrors() {
        return moveAfterErrors;
    }

    public String getMoveAfterFailure() {
        return moveAfterFailure;
    }

    public DateFormat getMoveTimestampFormat() {
        return moveTimestampFormat;
    }

    public String getCheckSizeInterval() {
        return checkSizeInterval;
    }

    public String getCheckSizeIgnoreEmpty() {
        return checkSizeIgnoreEmpty;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public long getReconnectTimeout() {
        return reconnectTimeout;
    }

    public boolean isFileLocking() {
        return fileLocking;
    }

    public CryptoUtil getCryptoUtil() {
        return cryptoUtil;
    }

    public Properties getSecureVaultProperties() {
        return secureVaultProperties;
    }

    public double getFileSizeLimit() {
        return fileSizeLimit;
    }

    public String getMoveAfterMoveFailure() {
        return moveAfterMoveFailure;
    }

    public int getNextRetryDurationForFailedMove() {
        return nextRetryDurationForFailedMove;
    }

    public String getFailedRecordFileName() {
        return failedRecordFileName;
    }

    public String getFailedRecordFileDestination() {
        return failedRecordFileDestination;
    }

    public String getFailedRecordTimestampFormat() {
        return failedRecordTimestampFormat;
    }

    public Integer getFileProcessingInterval() {
        return fileProcessingInterval;
    }

    public Integer getFileProcessingCount() {
        return fileProcessingCount;
    }

    public boolean isAutoLockRelease() {
        return autoLockRelease;
    }

    public Long getAutoLockReleaseInterval() {
        return autoLockReleaseInterval;
    }

    public Boolean getAutoLockReleaseSameNode() {
        return autoLockReleaseSameNode;
    }

    public boolean isDistributedLock() {
        return distributedLock;
    }

    public String getFileSortParam() {
        return fileSortParam;
    }

    public boolean isFileSortAscending() {
        return fileSortAscending;
    }

    public boolean isForceCreateFolder() {
        return forceCreateFolder;
    }

    public String getSubfolderTimestamp() {
        return subfolderTimestamp;
    }

    public Long getDistributedLockTimeout() {
        return distributedLockTimeout;
    }

    public boolean isCanceled() {
        return canceled;
    }

    public boolean isResolveHostsDynamically() {
        return resolveHostsDynamically;
    }

    public boolean isFileNotFoundLogged() {
        return fileNotFoundLogged;
    }

    public ParameterInclude getParams() {
        return params;
    }

    public Long getMinimumAge() {
        return minimumAge;
    }

    public Long getMaximumAge() {
        return maximumAge;
    }

    public boolean isBuild() {
        return build;
    }

    public String getReplayFileName() {
        return replayFileName;
    }

    public boolean isAppend() {
        return append;
    }

    public boolean isAvoidPermissionCheck() {
        return avoidPermissionCheck;
    }

    public boolean isPassive() {
        return passive;
    }

    public Long getFailedRecordNextRetryDuration() {
        return failedRecordNextRetryDuration;
    }
}
