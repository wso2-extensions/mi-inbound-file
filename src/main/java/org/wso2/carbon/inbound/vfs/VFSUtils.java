package org.wso2.carbon.inbound.vfs;

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;
import org.apache.synapse.commons.vfs.VFSConstants;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VFSUtils {

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

    public static FileSystemOptions attachFileSystemOptions(Map<String, String> options, FileSystemManager fsManager)
            throws FileSystemException {
        if (options == null) {
            return null;
        }

        FileSystemOptions opts = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder delegate = new DelegatingFileSystemOptionsBuilder(fsManager);

        // setting all available configs regardless of the options.get(VFSConstants.SCHEME)
        // because schemes of FileURI and MoveAfterProcess can be different

        //sftp configs
        for (Map.Entry<String, String> entry : options.entrySet()) {
            for (org.apache.synapse.commons.vfs.VFSConstants.SFTP_FILE_OPTION option : org.apache.synapse.commons.vfs.VFSConstants.SFTP_FILE_OPTION.values()) {
                if (entry.getKey().equals(option.toString()) && entry.getValue() != null) {
                    delegate.setConfigString(opts, org.apache.synapse.commons.vfs.VFSConstants.SCHEME_SFTP, entry.getKey().toLowerCase(),
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
//        String keyStore = options.get(KEY_STORE);
//        if (keyStore != null) {
//            configBuilder.setKeyStore(opts, keyStore);
//        }
//        String trustStore = options.get(TRUST_STORE);
//        if (trustStore != null) {
//            configBuilder.setTrustStore(opts, trustStore);
//        }
//        String keyStorePassword = options.get(KS_PASSWD);
//        if (keyStorePassword != null) {
//            configBuilder.setKeyStorePW(opts, keyStorePassword);
//        }
//        String trustStorePassword = options.get(TS_PASSWD);
//        if (trustStorePassword != null) {
//            configBuilder.setTrustStorePW(opts, trustStorePassword);
//        }
//        String keyPassword = options.get(KEY_PASSWD);
//        if (keyPassword != null) {
//            configBuilder.setKeyPW(opts, keyPassword);
//        }

        if (options.get(org.apache.synapse.commons.vfs.VFSConstants.FILE_TYPE) != null) {
            delegate.setConfigString(opts, options.get(
                            org.apache.synapse.commons.vfs.VFSConstants.SCHEME), org.apache.synapse.commons.vfs.VFSConstants.FILE_TYPE,
                    options.get(VFSConstants.FILE_TYPE));
        }

//        Smb2FileSystemConfigBuilder smb2FileSystemConfigBuilder = Smb2FileSystemConfigBuilder.getInstance();
//
//        boolean encryptionEnabled = Boolean.parseBoolean(options.get(ENCRYPTION_ENABLED));
//        if (options.get(ENCRYPTION_ENABLED) != null) {
//            smb2FileSystemConfigBuilder.setEncryptionEnabled(opts, encryptionEnabled);
//        }
//
//        String diskfileshare = options.get(DISK_SHARE_ACCESS_MASK);
//        if (diskfileshare != null) {
//            smb2FileSystemConfigBuilder.setDiskShareAccessMask(opts, validateAndGetDiskShareAccessMask(diskfileshare));
//        }

        return opts;
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

//    =============================REMOVE===============
//    TODO: remove it after syncing the commons-vfs fork and use that instead
    public static Map<String, String> extractQueryParams(String uri) throws FileSystemException {
        Map<String, String> sQueryParams = new HashMap();
        if (uri != null) {
            String[] urlParts = uri.split("\\?");
            if (urlParts.length > 1) {
                String query = urlParts[1];
                query = decode(query);

                for(String param : query.split("&")) {
                    String[] pair = param.split("=");
                    if (pair.length > 1) {
                        sQueryParams.put(pair[0], pair[1]);
                    }
                }
            }
        }

        return sQueryParams;
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
//    =====================================
}
