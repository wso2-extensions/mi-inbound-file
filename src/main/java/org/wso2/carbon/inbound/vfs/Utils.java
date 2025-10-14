package org.wso2.carbon.inbound.vfs;

import com.hierynomus.msdtyp.AccessMask;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.vfs.VFSConstants;
import org.wso2.carbon.inbound.vfs.processor.Action;
import org.wso2.carbon.inbound.vfs.processor.DeleteAction;
import org.wso2.carbon.inbound.vfs.processor.MoveAction;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;

import java.io.File;
import java.io.IOException;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    private static final Log log = LogFactory.getLog(Utils.class);

    private static final Pattern URL_PATTERN = Pattern.compile("[a-zA-Z0-9]+://.*");
    private static final Pattern PASSWORD_PATTERN = Pattern.compile(":(?:[^/]+)@");

    /**
     * SSL Keystore.
     */
    private static final String KEY_STORE = "vfs.ssl.keystore";

    /**
     * SSL Truststore.
     */
    private static final String TRUST_STORE = "vfs.ssl.truststore";

    /**
     * SSL Keystore password.
     */
    private static final String KS_PASSWD = "vfs.ssl.kspassword";

    /**
     * SSL Truststore password.
     */
    private static final String TS_PASSWD = "vfs.ssl.tspassword";

    /**
     * SSL Key password.
     */
    private static final String KEY_PASSWD = "vfs.ssl.keypassword";

    /**
     * Passive mode
     */
    public static final String PASSIVE_MODE = "vfs.passive";

    /**
     * FTPS implicit mode
     */
    public static final String IMPLICIT_MODE = "vfs.implicit";

    public static final String PROTECTION_MODE = "vfs.protection";

    private static final String ENCRYPTION_ENABLED = "vfs.EncryptionEnabled";

    public static final String DISK_SHARE_ACCESS_MASK = "vfs.diskShareAccessMask";

    public static final String DISK_SHARE_ACCESS_MASK_MAX_ALLOWED = "MAXIMUM_ALLOWED";

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
     * @param uri URI need to resolve
     * @return hostname resolved uri
     * @throws FileSystemException Unable to decode due to malformed URI
     * @throws UnknownHostException Error occurred while resolving hostname of URI
     */
    public static String resolveUriHost (String uri) throws FileSystemException, UnknownHostException {
        return resolveUriHost(uri, new StringBuilder());
    }

    /**
     * Function to resolve the hostname of uri to ip for following vfs protocols. if not the protocol listed, return
     * same uri provided for {uri}
     * Protocols resolved : SMB
     * @param uri URI need to resolve
     * @param strBuilder string builder to use to build the resulting uri
     * @return hostname resolved uri
     * @throws FileSystemException Unable to decode due to malformed URI
     * @throws UnknownHostException Error occurred while resolving hostname of URI
     */
    public static String resolveUriHost (String uri, StringBuilder strBuilder)
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
     * extracted hostname will be reoved from the StringBuilder
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

        for(int count = length; count > 0; ++index) {
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

                char value = (char)(dig1 << 4 | dig2);
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
            Long lDiff = 0l;
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
            Long lDiff = 0l;
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
            Long lDiff = 0l;
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
            Long lDiff = 0l;
            try {
                lDiff = o2.getContent().getSize() - o1.getContent().getSize();
            } catch (FileSystemException e) {
                log.warn("Unable to compare size of the two files.", e);
            }
            return lDiff.intValue();
        }
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
                }else{
                    moveToDirectoryURI += strDateformat;
                }
            } catch (Exception e) {
                log.warn("Error generating subfolder name with date", e);
            }
        }
        return moveToDirectoryURI;
    }

    public static synchronized void addFailedRecord(VFSConfig vfsConfig,
                                                    FileObject failedObject,
                                                    String timeString, FileSystemManager fsManager) {
        try {
            String record = failedObject.getName().getBaseName() + VFSConstants.FAILED_RECORD_DELIMITER
                    + timeString;
            String recordFile = vfsConfig.getFailedRecordFileDestination() +
                    vfsConfig.getFailedRecordFileName();
            File failedRecordFile = new File(recordFile);
            if (!failedRecordFile.exists()) {
                FileUtils.writeStringToFile(failedRecordFile, record);
                if (log.isDebugEnabled()) {
                    log.debug("Added fail record '"
                            + Utils.maskURLPassword(record.toString())
                            + "' into the record file '"
                            + recordFile + "'");
                }
            } else {
                List<String> content = FileUtils.readLines(failedRecordFile);
                if (!content.contains(record)) {
                    content.add(record);
                }
                FileUtils.writeLines(failedRecordFile, content);
            }
        } catch (IOException e) {
            VFSTransportErrorHandler.logException(log, VFSTransportErrorHandler.LogType.FATAL,
                    "Failure while writing the failed records!", e);
            markFailRecord(fsManager, failedObject);
        }
    }

    public static String getSystemTime(String dateFormat) {
        return (new SimpleDateFormat(dateFormat)).format(new Date());
    }

    public static synchronized void markFailRecord(FileSystemManager fsManager, FileObject fo) {
        markFailRecord(fsManager, fo, (FileSystemOptions)null);
    }

    public static synchronized void markFailRecord(FileSystemManager fsManager, FileObject fo, FileSystemOptions fso) {
        byte[] failValue = Long.toString((new Date()).getTime()).getBytes();

        try {
            String fullPath = getFullPath(fo);
            FileObject failObject = fsManager.resolveFile(fullPath + ".fail", fso);
            if (!failObject.exists()) {
                failObject.createFile();
            }

            OutputStream stream = failObject.getContent().getOutputStream();

            try {
                stream.write(failValue);
                stream.flush();
            } catch (IOException var17) {
                failObject.delete();
                log.error("Couldn't create the fail file before processing the file " + maskURLPassword(fullPath), var17);
            } finally {
                try {
                    stream.close();
                } catch (IOException var16) {
                    log.debug("Error closing stream", var16);
                }

                failObject.close();
            }
        } catch (FileSystemException var19) {
            log.error("Cannot get the lock for the file : " + maskURLPassword(fo.getName().getURI()) + " before processing");
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
        } else {
            FileSystemOptions opts = new FileSystemOptions();
            DelegatingFileSystemOptionsBuilder delegate = new DelegatingFileSystemOptionsBuilder(fsManager);
            Iterator var4 = options.entrySet().iterator();

            while(var4.hasNext()) {
                Map.Entry<String, String> entry = (Map.Entry)var4.next();
                VFSConstants.SFTP_FILE_OPTION[] var6 = VFSConstants.SFTP_FILE_OPTION.values();
                int var7 = var6.length;

                for(int var8 = 0; var8 < var7; ++var8) {
                    VFSConstants.SFTP_FILE_OPTION option = var6[var8];
                    if (((String)entry.getKey()).equals(option.toString()) && entry.getValue() != null) {
                        delegate.setConfigString(opts, "sftp", ((String)entry.getKey()).toLowerCase(), (String)entry.getValue());
                    }
                }
            }

            FtpsFileSystemConfigBuilder configBuilder = FtpsFileSystemConfigBuilder.getInstance();
            String passiveMode = (String)options.get("vfs.passive");
            if (passiveMode != null) {
                configBuilder.setPassiveMode(opts, Boolean.parseBoolean(passiveMode));
            }

            String implicitMode = (String)options.get("vfs.implicit");
            if (implicitMode != null) {
                if (Boolean.parseBoolean(implicitMode)) {
                    configBuilder.setFtpsMode(opts, FtpsMode.IMPLICIT);
                } else {
                    configBuilder.setFtpsMode(opts, FtpsMode.EXPLICIT);
                }
            }

            String protectionMode = (String)options.get("vfs.protection");
            if ("P".equalsIgnoreCase(protectionMode)) {
                configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.P);
            } else if ("C".equalsIgnoreCase(protectionMode)) {
                configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.C);
            } else if ("S".equalsIgnoreCase(protectionMode)) {
                configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.S);
            } else if ("E".equalsIgnoreCase(protectionMode)) {
                configBuilder.setDataChannelProtectionLevel(opts, FtpsDataChannelProtectionLevel.E);
            }

            String keyStore = (String)options.get("vfs.ssl.keystore");
            if (keyStore != null) {
                configBuilder.setKeyStore(opts, keyStore);
            }

            String trustStore = (String)options.get("vfs.ssl.truststore");
            if (trustStore != null) {
                configBuilder.setTrustStore(opts, trustStore);
            }

            String keyStorePassword = (String)options.get("vfs.ssl.kspassword");
            if (keyStorePassword != null) {
                configBuilder.setKeyStorePW(opts, keyStorePassword);
            }

            String trustStorePassword = (String)options.get("vfs.ssl.tspassword");
            if (trustStorePassword != null) {
                configBuilder.setTrustStorePW(opts, trustStorePassword);
            }

            String keyPassword = (String)options.get("vfs.ssl.keypassword");
            if (keyPassword != null) {
                configBuilder.setKeyPW(opts, keyPassword);
            }

            if (options.get("filetype") != null) {
                delegate.setConfigString(opts, (String)options.get("VFS_SCHEME"), "filetype", (String)options.get("filetype"));
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
            int pos = fullPath.indexOf(63);
            if (pos > -1) {
                queryParams = fullPath.substring(pos);
                fullPath = fullPath.substring(0, pos);
            }

            FileObject failObject = fsManager.resolveFile(fullPath + ".fail" + queryParams, fso);
            if (failObject.exists()) {
                return true;
            }
        } catch (FileSystemException var7) {
            log.error("Couldn't release the fail for the file : " + maskURLPassword(fo.getName().getURI()));
        }

        return false;
    }

    public static boolean isFailedRecordInFailedList(FileObject fileObject, VFSConfig entry) {
        String failedFile = entry.getFailedRecordFileDestination() +
                entry.getFailedRecordFileName();
        File file = new File(failedFile);
        if (file.exists()) {
            try {
                List list = FileUtils.readLines(file);
                for (Object aList : list) {
                    String str = (String) aList;
                    StringTokenizer st = new StringTokenizer(str,
                            org.wso2.carbon.inbound.vfs.VFSConstants.FAILED_RECORD_DELIMITER);
                    String fileName = st.nextToken();
                    if (fileName != null &&
                            fileName.equals(fileObject.getName().getBaseName())) {
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
        return uri; // no parameters
    }


    public static void releaseLock(FileSystemManager fsManager, FileObject fo, FileSystemOptions fso) {
        String fullPath = fo.getName().getURI();

        try {
            int pos = fullPath.indexOf(63);
            if (pos > -1) {
                fullPath = fullPath.substring(0, pos);
            }

            FileObject lockObject = fsManager.resolveFile(fullPath + ".lock", fso);
            if (lockObject.exists()) {
                lockObject.delete();
            }
        } catch (FileSystemException var8) {
            log.error("Couldn't release the lock for the file : " + maskURLPassword(fo.getName().getURI()) + " after processing");

            try {
                ((DefaultFileSystemManager)fsManager).closeCachedFileSystem(fullPath + ".lock", fso);
            } catch (Exception var7) {
                log.warn("Unable to clear file system", var7);
            }
        }

    }

}
