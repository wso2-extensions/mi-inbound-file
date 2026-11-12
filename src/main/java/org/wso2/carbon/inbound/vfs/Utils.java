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

import com.hierynomus.msdtyp.AccessMask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.processor.Action;
import org.wso2.carbon.inbound.vfs.processor.DeleteAction;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;

import static org.wso2.carbon.inbound.vfs.VFSConstants.DISK_SHARE_ACCESS_MASK;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DISK_SHARE_ACCESS_MASK_MAX_ALLOWED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.ENCRYPTION_ENABLED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FAIL_FILE_SUFFIX;
import static org.wso2.carbon.inbound.vfs.VFSConstants.IMPLICIT_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KEY_PASSWD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KEY_STORE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KS_PASSWD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.PASSIVE_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.PASSWORD_PATTERN;
import static org.wso2.carbon.inbound.vfs.VFSConstants.PROTECTION_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRUST_STORE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TS_PASSWD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.URL_PATTERN;

public class Utils {

    private static final Log log = LogFactory.getLog(Utils.class);

    public static String maskURLPassword(String url) {
        Matcher urlMatcher = URL_PATTERN.matcher(url);
        if (urlMatcher.find()) {
            Matcher pwdMatcher = PASSWORD_PATTERN.matcher(url);
            return pwdMatcher.replaceFirst(":***@");
        } else {
            return url;
        }
    }

    /**
     * Function to resolve hostname of the vfs uri
     *
     * @param uri URI need to resolve
     * @return hostname resolved uri
     * @throws FileSystemException  Unable to decode due to malformed URI
     * @throws UnknownHostException Error occurred while resolving hostname of URI
     */
    public static String resolveUriHost(String uri) throws FileSystemException, UnknownHostException {
        return resolveUriHost(uri, new StringBuilder());
    }

    /**
     * Function to resolve the hostname of uri to ip for following vfs protocols. if not the protocol listed, return
     * same uri provided for {uri}
     * Protocols resolved : SMB
     *
     * @param uri        URI need to resolve
     * @param strBuilder string builder to use to build the resulting uri
     * @return hostname resolved uri
     * @throws FileSystemException  Unable to decode due to malformed URI
     * @throws UnknownHostException Error occurred while resolving hostname of URI
     */
    public static String resolveUriHost(String uri, StringBuilder strBuilder)
            throws FileSystemException, UnknownHostException {

        if (uri != null && strBuilder != null) {
            // Extract the scheme
            String scheme = UriParser.extractScheme(uri, strBuilder);

            //need to resolve hosts of smb URIs due to limitation in jcifs library
            if (scheme != null && (scheme.equals("smb"))) {
                // Expecting "//"
                if (strBuilder.length() < 2 || strBuilder.charAt(0) != '/' || strBuilder.charAt(1) != '/') {
                    throw new FileSystemException("vfs.provider/missing-double-slashes.error", uri);
                }
                strBuilder.delete(0, 2);

                // Extract userinfo
                String userInfo = extractUserInfo(strBuilder);

                // Extract hostname
                String hostName = extractHostName(strBuilder);

                //resolve host name
                InetAddress hostAddress = InetAddress.getByName(hostName);
                String resolvedHostAddress = hostAddress.getHostAddress();

                //build resolved uri
                StringBuilder uriStrBuilder = new StringBuilder();
                uriStrBuilder.append(scheme).append("://");

                if (userInfo != null) {
                    //user information can be null since it's optional
                    uriStrBuilder.append(userInfo).append("@");
                }

                uriStrBuilder.append(resolvedHostAddress).append(strBuilder);

                return uriStrBuilder.toString();
            }
        }

        return uri;
    }

    /**
     * Extracts the hostname from a URI.  The scheme://userinfo@ part has
     * been removed.
     * extracted hostname will be removed from the StringBuilder
     */
    private static String extractHostName(StringBuilder name) {
        final int maxlen = name.length();
        int pos = 0;
        for (; pos < maxlen; pos++) {
            char ch = name.charAt(pos);
            //if /;?:@&=+$, characters found means, we have passed the hostname, hence break
            if (ch == '/' || ch == ';' || ch == '?' || ch == ':'
                    || ch == '@' || ch == '&' || ch == '=' || ch == '+'
                    || ch == '$' || ch == ',') {
                break;
            }
        }
        if (pos == 0) {
            //haven't found the hostname
            return null;
        }

        String hostname = name.substring(0, pos);
        name.delete(0, pos);
        return hostname;
    }

    /**
     * Extracts the user info from a URI.  The scheme:// part has been removed
     * already.
     * extracted user info will be removed from the StringBuilder
     */
    private static String extractUserInfo(StringBuilder name) {
        int maxlen = name.length();
        for (int pos = 0; pos < maxlen; pos++) {
            char ch = name.charAt(pos);
            if (ch == '@') {
                // Found the end of the user info
                String userInfo = name.substring(0, pos);
                name.delete(0, pos + 1);
                return userInfo;
            }
            if (ch == '/' || ch == '?') {
                // Not allowed in user info
                break;
            }
        }

        // Not found
        return null;
    }

    public static String decode(String encodedStr) throws FileSystemException {
        if (encodedStr == null) {
            return null;
        } else if (encodedStr.indexOf(37) < 0) {
            return encodedStr;
        } else {
            StringBuilder buffer = new StringBuilder(encodedStr);
            decode(buffer, 0, buffer.length());
            return buffer.toString();
        }
    }

    public static void decode(StringBuilder buffer, int offset, int length) throws FileSystemException {
        int index = offset;

        for (int count = length; count > 0; ++index) {
            char ch = buffer.charAt(index);
            if (ch == '%') {
                if (count < 3) {
                    throw new FileSystemException("vfs.provider/invalid-escape-sequence.error", buffer.substring(index, index + count));
                }

                int dig1 = Character.digit(buffer.charAt(index + 1), 16);
                int dig2 = Character.digit(buffer.charAt(index + 2), 16);
                if (dig1 == -1 || dig2 == -1) {
                    throw new FileSystemException("vfs.provider/invalid-escape-sequence.error", buffer.substring(index, index + 3));
                }

                char value = (char) (dig1 << 4 | dig2);
                buffer.setCharAt(index, value);
                buffer.delete(index + 1, index + 3);
                count -= 2;
            }

            --count;
        }
    }


    public static boolean supportsSubDirectoryToken(String original) {
        String[] parts = original.split("\\?");
        // Either '/*' or '\*' before query string
        return parts[0].endsWith("/*") || parts[0].endsWith("\\*");
    }

    public static String sanitizeFileUriWithSub(String original) {
        int INCLUDE_SUB_DIR_SYMBOL_LENGTH = 2;
        String[] parts = original.split("\\?");
        parts[0] = parts[0].substring(0, parts[0].length() - INCLUDE_SUB_DIR_SYMBOL_LENGTH);
        return parts.length == 1 ? parts[0] : parts[0] + "?" + parts[1];
    }

    public static Action getActionAfterProcess(VFSConfig vfsConfig, int actionAfterProcess, String targetLocation, FileSystemManager fsManager) {
        switch (actionAfterProcess) {
            case VFSConfig.DELETE:
                return new DeleteAction();
            case VFSConfig.MOVE:
                return new MoveAction(targetLocation, vfsConfig, fsManager);
            default:
                return null;
        }
    }

    public static FileObject[] sortFileObjects(FileObject[] children, String strSortParam, VFSConfig vfsProperties) {
        if (strSortParam != null && !"NONE".equals(strSortParam)) {
            log.debug("Start Sorting the files.");
            boolean strSortOrder = vfsProperties.isFileSortAscending();
            if (log.isDebugEnabled()) {
                log.debug("Sorting the files by : " + strSortOrder + ". (" + strSortOrder + ")");
            }
            if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_NAME) && strSortOrder) {
                Arrays.sort(children, new FileNameAscComparator());
            } else if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_NAME)) {
                Arrays.sort(children, new FileNameDesComparator());
            } else if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_SIZE) && strSortOrder) {
                Arrays.sort(children, new FileSizeAscComparator());
            } else if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_SIZE)) {
                Arrays.sort(children, new FileSizeDesComparator());
            } else if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP)
                    && strSortOrder) {
                Arrays.sort(children, new FileLastmodifiedtimestampAscComparator());
            } else if (strSortParam.equals(VFSConstants.FILE_SORT_VALUE_LASTMODIFIEDTIMESTAMP)) {
                Arrays.sort(children, new FileLastmodifiedtimestampDesComparator());
            }
            log.debug("End Sorting the files.");
        }
        return children;
    }

    public static String optionallyAppendDateToUri(String moveToDirectoryURI, VFSConfig vfsConfig) {
        String strSubfoldertimestamp = vfsConfig.getSubfolderTimestamp();
        if (strSubfoldertimestamp != null) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(strSubfoldertimestamp);
                String strDateformat = sdf.format(new Date());
                int iIndex = moveToDirectoryURI.indexOf("?");
                if (iIndex > -1) {
                    moveToDirectoryURI = moveToDirectoryURI.substring(0, iIndex)
                            + strDateformat
                            + moveToDirectoryURI.substring(iIndex);
                } else {
                    moveToDirectoryURI += strDateformat;
                }
            } catch (Exception e) {
                log.warn("Error generating subfolder name with date", e);
            }
        }
        return moveToDirectoryURI;
    }

    /**
     * Adds the file to the failed records file.
     *
     * @return false if the failed records file could not be written (the caller records the failure another way)
     */
    public static synchronized boolean addFailedRecord(VFSConfig vfsConfig, FileObject failedObject,
                                                       String timeString) {
        try {
            // keyed by the full (password-masked) URI, so that files with the same name in other folders are
            // not affected
            String key = failedRecordKey(failedObject);
            String record = key + VFSConstants.FAILED_RECORD_DELIMITER + timeString;
            String recordFile = vfsConfig.getFailedRecordFileDestination() +
                    vfsConfig.getFailedRecordFileName();
            File failedRecordFile = new File(recordFile);
            if (!failedRecordFile.exists()) {
                FileUtils.writeStringToFile(failedRecordFile, record);
                if (log.isDebugEnabled()) {
                    log.debug("Added fail record '"
                            + Utils.maskURLPassword(record)
                            + "' into the record file '"
                            + recordFile + "'");
                }
            } else {
                List<String> content = FileUtils.readLines(failedRecordFile);
                for (String line : content) {
                    if (isRecordOf(line, key, null)) {
                        return true;     // already recorded
                    }
                }
                content.add(record);
                FileUtils.writeLines(failedRecordFile, content);
            }
            return true;
        } catch (IOException e) {
            VFSTransportErrorHandler.logException(log, VFSTransportErrorHandler.LogType.FATAL,
                    "Failure while writing the failed records!", e);
            return false;
        }
    }

    /**
     * Key of a file in the failed records file: its URI without query parameters, with the password masked.
     */
    private static String failedRecordKey(FileObject fo) {
        return maskURLPassword(getFullPath(fo));
    }

    /**
     * Whether a failed records line belongs to the given key. Lines written by earlier versions start with the
     * base name only; they are matched by {@code legacyBaseName} when it is given.
     */
    private static boolean isRecordOf(String line, String key, String legacyBaseName) {
        String delimiter = VFSConstants.FAILED_RECORD_DELIMITER;
        return line.startsWith(key + delimiter)
                || (legacyBaseName != null && line.startsWith(legacyBaseName + delimiter));
    }

    public static String getSystemTime(String dateFormat) {
        return (new SimpleDateFormat(dateFormat)).format(new Date());
    }

    /**
     * Creates (or rewrites) the {@code <file>.fail} marker. The content is the current time in milliseconds and,
     * when {@code failedAction} is given, {@code :<failedAction>:<lastModified>:<size>} of the data file, so that a
     * later poll can retry only that action and can tell whether the file was replaced by a new one.
     * Only the existence of the marker is checked to skip the file.
     */
    public static synchronized void markFailRecord(FileSystemManager fsManager, FileObject fo, FileSystemOptions fso,
                                                   String failedAction) {

        String failText = Long.toString((new Date()).getTime());
        if (failedAction != null) {
            failText += VFSConstants.STR_SPLITER + failedAction;
            long[] identity = getFileIdentity(fo);
            if (identity != null) {
                failText += VFSConstants.STR_SPLITER + identity[0] + VFSConstants.STR_SPLITER + identity[1];
            }
        }
        byte[] failValue = failText.getBytes();

        try {
            String fullPath = getFullPath(fo);
            FileObject failObject = fsManager.resolveFile(fullPath + FAIL_FILE_SUFFIX, fso);
            if (!failObject.exists()) {
                failObject.createFile();
            }

            // write a lock file before starting of the processing, to ensure that the
            // item is not processed by any other parties

            OutputStream stream = failObject.getContent().getOutputStream();
            try {
                stream.write(failValue);
                stream.flush();
            } catch (IOException e) {
                failObject.delete();
                log.error("Couldn't create the fail file before processing the file " + maskURLPassword(fullPath), e);
            } finally {
                try {
                    stream.close();
                } catch (IOException e) {
                    log.debug("Error closing stream", e);
                }
                failObject.close();
            }
        } catch (FileSystemException fse) {
            log.error("Cannot get the lock for the file : " + maskURLPassword(fo.getName().getURI()) + " before processing");
        }
    }

    /**
     * Returns {lastModified, size} of the file, or null if they cannot be read.
     */
    public static long[] getFileIdentity(FileObject fo) {
        try {
            return new long[]{fo.getContent().getLastModifiedTime(), fo.getContent().getSize()};
        } catch (FileSystemException e) {
            if (log.isDebugEnabled()) {
                log.debug("Couldn't read the last modified time and size of " + maskURLPassword(fo.toString()), e);
            }
            return null;
        }
    }

    /**
     * Content of a {@code .fail} marker: {@code <markedAt>[:<action>[:<lastModified>:<size>]]}.
     * Markers written by earlier versions contain only {@code <markedAt>}.
     */
    public static final class FailRecord {
        private static final java.util.regex.Pattern FORMAT = java.util.regex.Pattern.compile(
                "^(\\d{13,})(?::(" + VFSConstants.FAILED_ACTION_SUCCESS + "|" + VFSConstants.FAILED_ACTION_FAILURE
                        + "|" + VFSConstants.FAILED_ACTION_UNKNOWN + ")(?::(-?\\d+):(-?\\d+))?)?$");

        public final long markedAt;
        public final String action;          // null for markers of earlier versions
        public final Long lastModified;      // null if not recorded
        public final Long size;              // null if not recorded

        private FailRecord(long markedAt, String action, Long lastModified, Long size) {
            this.markedAt = markedAt;
            this.action = action;
            this.lastModified = lastModified;
            this.size = size;
        }

        /**
         * Parses marker content; returns null if the content is not a fail record (for example a data file that
         * happens to end with {@code .fail}).
         */
        public static FailRecord parse(String content) {
            if (content == null) {
                return null;
            }
            Matcher m = FORMAT.matcher(content.trim());
            if (!m.matches()) {
                return null;
            }
            try {
                return new FailRecord(Long.parseLong(m.group(1)), m.group(2),
                        m.group(3) == null ? null : Long.valueOf(m.group(3)),
                        m.group(4) == null ? null : Long.valueOf(m.group(4)));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        /**
         * Whether the given file is still the file this record was written for. True when the identity was not
         * recorded or cannot be read now.
         */
        public boolean isSameFile(FileObject fo) {
            if (lastModified == null || size == null) {
                return true;
            }
            long[] identity = getFileIdentity(fo);
            return identity == null || (identity[0] == lastModified && identity[1] == size);
        }
    }

    /**
     * Returns the content of the {@code <file>.fail} marker, or null if there is no marker or it cannot be read.
     */
    public static String readFailRecord(FileSystemManager fsManager, FileObject fo, FileSystemOptions fso) {
        return readMarker(fsManager, getFullPath(fo) + FAIL_FILE_SUFFIX, fso);
    }

    private static String readMarker(FileSystemManager fsManager, String markerPath, FileSystemOptions fso) {
        try {
            FileObject failObject = fsManager.resolveFile(markerPath, fso);
            try {
                if (!failObject.exists()) {
                    return null;
                }
                try (InputStream is = failObject.getContent().getInputStream()) {
                    return new String(IOUtils.toByteArray(is)).trim();
                }
            } finally {
                failObject.close();
            }
        } catch (IOException e) {
            log.error("Couldn't read the fail record : " + maskURLPassword(markerPath), e);
            return null;
        }
    }

    /**
     * Deletes the {@code <file>.fail} marker of the given file, if it exists.
     */
    public static synchronized void releaseFailRecord(FileSystemManager fsManager, FileObject fo,
                                                      FileSystemOptions fso) {
        String fullPath = getFullPath(fo);
        try {
            FileObject failObject = fsManager.resolveFile(fullPath + FAIL_FILE_SUFFIX, fso);
            try {
                if (failObject.exists()) {
                    failObject.delete();
                }
            } finally {
                failObject.close();
            }
        } catch (FileSystemException e) {
            log.error("Couldn't remove the fail record of the file : " + maskURLPassword(fullPath), e);
        }
    }

    /**
     * Deletes a {@code .fail} marker whose data file no longer exists, so that it does not block a later file
     * with the same name. Files that end with {@code .fail} but are not fail records are left alone.
     *
     * @param failMarker the {@code <file>.fail} file object
     */
    public static synchronized void releaseOrphanFailRecord(FileSystemManager fsManager, FileObject failMarker,
                                                            FileSystemOptions fso) {
        String markerPath = getFullPath(failMarker);
        if (!markerPath.endsWith(FAIL_FILE_SUFFIX)) {
            return;
        }
        String dataPath = markerPath.substring(0, markerPath.length() - FAIL_FILE_SUFFIX.length());
        try {
            FileObject dataFile = fsManager.resolveFile(dataPath, fso);
            try {
                if (dataFile.exists() || !failMarker.exists()) {
                    return;
                }
                if (FailRecord.parse(readMarker(fsManager, markerPath, fso)) == null) {
                    return;     // not a fail record
                }
                failMarker.delete();
                log.info("Removed the fail record of " + maskURLPassword(dataPath)
                        + " as the file no longer exists");
            } finally {
                dataFile.close();
            }
        } catch (FileSystemException e) {
            log.error("Couldn't check or remove the fail record : " + maskURLPassword(markerPath), e);
        }
    }

    /**
     * Deletes the {@code .fail} marker of a data file that no longer exists (used when the FileURI is a single
     * file, so the marker is not seen in a directory listing).
     */
    public static void releaseOrphanFailRecordOf(FileSystemManager fsManager, FileObject dataFile,
                                                 FileSystemOptions fso) {
        try {
            FileObject failMarker = fsManager.resolveFile(getFullPath(dataFile) + FAIL_FILE_SUFFIX, fso);
            try {
                releaseOrphanFailRecord(fsManager, failMarker, fso);
            } finally {
                failMarker.close();
            }
        } catch (FileSystemException e) {
            log.error("Couldn't check the fail record of : " + maskURLPassword(getFullPath(dataFile)), e);
        }
    }

    /**
     * Removes the entries of the given file from the failed records file (including entries written by earlier
     * versions, which contain only the base name).
     */
    public static synchronized void removeFailedRecord(VFSConfig vfsConfig, FileObject fo) {
        String recordFile = vfsConfig.getFailedRecordFileDestination() + vfsConfig.getFailedRecordFileName();
        File failedRecordFile = new File(recordFile);
        if (!failedRecordFile.exists()) {
            return;
        }
        String key = failedRecordKey(fo);
        try {
            List<String> content = FileUtils.readLines(failedRecordFile);
            List<String> remaining = new ArrayList<>();
            for (String line : content) {
                if (!isRecordOf(line, key, fo.getName().getBaseName())) {
                    remaining.add(line);
                }
            }
            if (remaining.size() != content.size()) {
                FileUtils.writeLines(failedRecordFile, remaining);
                if (log.isDebugEnabled()) {
                    log.debug("Removed the fail record of '" + key + "' from the record file '" + recordFile + "'");
                }
            }
        } catch (IOException e) {
            log.error("Failure while removing '" + key + "' from the failed records file " + recordFile, e);
        }
    }

    public static String getFullPath(FileObject fo) {
        String fullPath = fo.getName().getURI();
        int pos = fullPath.indexOf(63);
        if (pos != -1) {
            fullPath = fullPath.substring(0, pos);
        }

        return fullPath;
    }

    public static FileSystemOptions attachFileSystemOptions(Map<String, String> options, FileSystemManager fsManager) throws FileSystemException {
        if (options == null) {
            return null;
        }

        FileSystemOptions opts = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder delegate = new DelegatingFileSystemOptionsBuilder(fsManager);

        // setting all available configs regardless of the options.get(VFSConstants.SCHEME)
        // because schemes of FileURI and MoveAfterProcess can be different

        //sftp configs
        for (Map.Entry<String, String> entry : options.entrySet()) {
            for (VFSConstants.SFTP_FILE_OPTION option : VFSConstants.SFTP_FILE_OPTION.values()) {
                if (entry.getKey().equals(option.toString()) && entry.getValue() != null) {
                    delegate.setConfigString(opts, VFSConstants.SCHEME_SFTP, entry.getKey().toLowerCase(),
                            entry.getValue());
                }
            }
        }

        FtpsFileSystemConfigBuilder configBuilder = FtpsFileSystemConfigBuilder.getInstance();

        // ftp and ftps configs
        String passiveMode = options.get(PASSIVE_MODE);
        if (passiveMode != null) {
            configBuilder.setPassiveMode(opts, Boolean.parseBoolean(passiveMode));
        }

        // ftps configs
        String implicitMode = options.get(IMPLICIT_MODE);
        if (implicitMode != null) {
            if (Boolean.parseBoolean(implicitMode)) {
                configBuilder.setFtpsMode(opts, FtpsMode.IMPLICIT);
            } else {
                configBuilder.setFtpsMode(opts, FtpsMode.EXPLICIT);
            }
        }
        String protectionMode = options.get(PROTECTION_MODE);
        if ("P".equalsIgnoreCase(protectionMode)) {
            configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.P);
        } else if ("C".equalsIgnoreCase(protectionMode)) {
            configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.C);
        } else if ("S".equalsIgnoreCase(protectionMode)) {
            configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.S);
        } else if ("E".equalsIgnoreCase(protectionMode)) {
            configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.E);
        }
        String keyStore = options.get(KEY_STORE);
        if (keyStore != null) {
            configBuilder.setKeyStore(opts, keyStore);
        }
        String trustStore = options.get(TRUST_STORE);
        if (trustStore != null) {
            configBuilder.setTrustStore(opts, trustStore);
        }
        String keyStorePassword = options.get(KS_PASSWD);
        if (keyStorePassword != null) {
            configBuilder.setKeyStorePW(opts, keyStorePassword);
        }
        String trustStorePassword = options.get(TS_PASSWD);
        if (trustStorePassword != null) {
            configBuilder.setTrustStorePW(opts, trustStorePassword);
        }
        String keyPassword = options.get(KEY_PASSWD);
        if (keyPassword != null) {
            configBuilder.setKeyPW(opts, keyPassword);
        }

        if (options.get(VFSConstants.FILE_TYPE) != null) {
            delegate.setConfigString(opts, options.get(VFSConstants.SCHEME), VFSConstants.FILE_TYPE,
                    options.get(VFSConstants.FILE_TYPE));
        }

        Smb2FileSystemConfigBuilder smb2FileSystemConfigBuilder = Smb2FileSystemConfigBuilder.getInstance();

        boolean encryptionEnabled = Boolean.parseBoolean(options.get(ENCRYPTION_ENABLED));
        if (options.get(ENCRYPTION_ENABLED) != null) {
            smb2FileSystemConfigBuilder.setEncryptionEnabled(opts, encryptionEnabled);
        }

        String diskfileshare = options.get(DISK_SHARE_ACCESS_MASK);
        if (diskfileshare != null) {
            smb2FileSystemConfigBuilder.setDiskShareAccessMask(opts, validateAndGetDiskShareAccessMask(diskfileshare));
        }

        return opts;
    }

    public static ArrayList<String> validateAndGetDiskShareAccessMask(String diskShareAccessMask) {
        // Prepare the set of allowed access mask values
        Set<String> allowedValues = new HashSet<String>();
        for (AccessMask mask : AccessMask.values()) {
            allowedValues.add(mask.name());
        }

        // Assign the validated value
        ArrayList<String> outDiskShareAccessMasks = new ArrayList<String>();

        // Validate and collect allowed values
        if (diskShareAccessMask != null) {
            String[] masks = diskShareAccessMask.split(",");
            for (String mask : masks) {
                String accessMask = mask.trim();
                if (allowedValues.contains(accessMask)) {
                    outDiskShareAccessMasks.add(accessMask);
                } else {
                    log.warn("Access mask is not valid and was ignored: " + mask);
                }
            }
        }

        // Fallback to default if nothing is valid or input was null
        if (outDiskShareAccessMasks.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Set the access mask to default MAXIMUM_ALLOWED since the access mask is not defined or the defined values are not valid.");
            }
            outDiskShareAccessMasks.add(DISK_SHARE_ACCESS_MASK_MAX_ALLOWED);
        }

        return outDiskShareAccessMasks;
    }

    public static boolean isFailRecord(FileSystemManager fsManager, FileObject fo, FileSystemOptions fso) {
        try {
            String fullPath = fo.getName().getURI();
            String queryParams = "";
            int pos = fullPath.indexOf('?');
            if (pos > -1) {
                queryParams = fullPath.substring(pos);
                fullPath = fullPath.substring(0, pos);
            }
            FileObject failObject = fsManager.resolveFile(fullPath + FAIL_FILE_SUFFIX + queryParams, fso);
            if (failObject.exists()) {
                return true;
            }
        } catch (FileSystemException e) {
            log.error("Couldn't release the fail for the file : " + maskURLPassword(fo.getName().getURI()));
        }
        return false;
    }

    public static boolean isFailedRecordInFailedList(FileObject fileObject, VFSConfig entry) {
        String failedFile = entry.getFailedRecordFileDestination() +
                entry.getFailedRecordFileName();
        File file = new File(failedFile);
        if (file.exists()) {
            String key = failedRecordKey(fileObject);
            String baseName = fileObject.getName().getBaseName();
            try {
                for (String line : FileUtils.readLines(file)) {
                    if (isRecordOf(line, key, baseName)) {
                        return true;
                    }
                }
            } catch (IOException e) {
                log.fatal("Error while reading the file '"
                        + Utils.maskURLPassword(failedFile) + "'", e);
            }
        }
        return false;
    }

    public static String extractPath(String uri) {
        int queryIndex = uri.indexOf('?');
        if (queryIndex != -1) {
            return uri.substring(0, queryIndex);
        }
        return uri;
    }


    public static String stripVfsSchemeIfPresent(String uri) {
        return uri != null && uri.startsWith("vfs:") ? uri.substring(4) : uri;
    }

    public static Map<String, String> parseSchemeFileOptions(String fileURI, Properties vfsProperties) {
        String scheme = UriParser.extractScheme(fileURI);
        if (scheme == null) {
            return null;
        } else {
            Map<String, String> schemeFileOptions = parseSchemeFileOptions(scheme, fileURI);
            addOptions(schemeFileOptions, vfsProperties);
            return schemeFileOptions;
        }
    }

    private static Map<String, String> parseSchemeFileOptions(String scheme, String fileURI) {
        HashMap<String, String> schemeFileOptions = new HashMap<>();
        schemeFileOptions.put(VFSConstants.SCHEME, scheme);
        try {
            Map<String, String> queryParams = UriParser.extractQueryParams(fileURI);
            schemeFileOptions.putAll(queryParams);
        } catch (FileSystemException e) {
            log.error("Error while loading scheme query params", e);
        }
        return schemeFileOptions;
    }

    private static void addOptions(Map<String, String> schemeFileOptions, Properties vfsProperties) {
        for (VFSConstants.SFTP_FILE_OPTION option : VFSConstants.SFTP_FILE_OPTION.values()) {
            String paramValue = vfsProperties.getProperty(
                    VFSConstants.SFTP_PREFIX + WordUtils.capitalize(option.toString()));
            if (paramValue != null && !paramValue.isEmpty()) {
                schemeFileOptions.put(option.toString(), paramValue);
            }
        }
    }

    /**
     * Comparator classed used to sort the files according to user input
     */
    public static class FileNameAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            return o1.getName().compareTo(o2.getName());
        }
    }

    static class FileLastmodifiedtimestampAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0L;
            try {
                lDiff = o1.getContent().getLastModifiedTime() - o2.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.warn("Unable to compare lastmodified timestamp of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    static class FileSizeAscComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0L;
            try {
                lDiff = o1.getContent().getSize() - o2.getContent().getSize();
            } catch (FileSystemException e) {
                log.warn("Unable to compare size of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    static class FileNameDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            return o2.getName().compareTo(o1.getName());
        }
    }

    static class FileLastmodifiedtimestampDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0L;
            try {
                lDiff = o2.getContent().getLastModifiedTime() - o1.getContent().getLastModifiedTime();
            } catch (FileSystemException e) {
                log.warn("Unable to compare lastmodified timestamp of the two files.", e);
            }
            return lDiff.intValue();
        }
    }

    static class FileSizeDesComparator implements Comparator<FileObject> {
        @Override
        public int compare(FileObject o1, FileObject o2) {
            Long lDiff = 0L;
            try {
                lDiff = o2.getContent().getSize() - o1.getContent().getSize();
            } catch (FileSystemException e) {
                log.warn("Unable to compare size of the two files.", e);
            }
            return lDiff.intValue();
        }
    }
}
