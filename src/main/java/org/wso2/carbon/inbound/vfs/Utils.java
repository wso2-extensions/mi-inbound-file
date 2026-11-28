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
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;
import org.apache.commons.logging.LogFactory;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;
import org.wso2.org.apache.commons.vfs2.FileSystemOptions;
import org.wso2.org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsDataChannelProtectionLevel;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.ftps.FtpsMode;
import org.wso2.org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.provider.smb2.Smb2FileSystemConfigBuilder;
import org.wso2.org.apache.commons.vfs2.util.DelegatingFileSystemOptionsBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.wso2.carbon.inbound.vfs.VFSConstants.DISK_SHARE_ACCESS_MASK;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DISK_SHARE_ACCESS_MASK_MAX_ALLOWED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.ENCRYPTION_ENABLED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.IMPLICIT_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KEY_PASSWD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KEY_STORE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.KS_PASSWD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.PASSIVE_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.PROTECTION_MODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRUST_STORE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TS_PASSWD;

public class Utils {

    private static final NamedLog log = NamedLog.inbound(LogFactory.getLog(Utils.class), "Utils", new RepeatLimiter());

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

    public static FileSystemOptions attachFileSystemOptions(Map<String, String> options, FileSystemManager fsManager) throws FileSystemException {
        return attachFileSystemOptions(options, fsManager, log);
    }

    public static FileSystemOptions attachFileSystemOptions(Map<String, String> options, FileSystemManager fsManager,
                                                           NamedLog log) throws FileSystemException {
        if (options == null) {
            return null;
        }

        FileSystemOptions opts = new FileSystemOptions();
        DelegatingFileSystemOptionsBuilder delegate = new DelegatingFileSystemOptionsBuilder(fsManager);

        String scheme = options.get(VFSConstants.SCHEME);

        //sftp configs
        for (Map.Entry<String, String> entry : options.entrySet()) {
            for (VFSConstants.SFTP_FILE_OPTION option : VFSConstants.SFTP_FILE_OPTION.values()) {
                if (entry.getKey().equals(option.toString()) && entry.getValue() != null) {
                    delegate.setConfigString(opts, VFSConstants.SCHEME_SFTP, entry.getKey().toLowerCase(Locale.ROOT),
                            entry.getValue());
                }
            }
        }

        applyConnectionOptions(opts, options, scheme);

        FtpsFileSystemConfigBuilder configBuilder = FtpsFileSystemConfigBuilder.getInstance();

        // ftp and ftps configs
        String passiveMode = options.get(PASSIVE_MODE);
        if (passiveMode != null) {
            FtpFileSystemConfigBuilder ftp = VFSConstants.SCHEME_FTP.equalsIgnoreCase(scheme)
                    ? FtpFileSystemConfigBuilder.getInstance() : configBuilder;
            ftp.setPassiveMode(opts, Boolean.parseBoolean(passiveMode));
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

        if (options.get(VFSConstants.FILE_TYPE) != null && (VFSConstants.SCHEME_FTP.equalsIgnoreCase(scheme)
                || VFSConstants.SCHEME_FTPS.equalsIgnoreCase(scheme))) {
            FtpFileSystemConfigBuilder ftp = VFSConstants.SCHEME_FTPS.equalsIgnoreCase(scheme)
                    ? configBuilder : FtpFileSystemConfigBuilder.getInstance();
            ftp.setFileType(opts, FtpFileType.valueOf(options.get(VFSConstants.FILE_TYPE).toUpperCase(Locale.ROOT)));
        }

        Smb2FileSystemConfigBuilder smb2FileSystemConfigBuilder = Smb2FileSystemConfigBuilder.getInstance();

        boolean encryptionEnabled = Boolean.parseBoolean(options.get(ENCRYPTION_ENABLED));
        if (options.get(ENCRYPTION_ENABLED) != null) {
            smb2FileSystemConfigBuilder.setEncryptionEnabled(opts, encryptionEnabled);
        }

        String diskfileshare = options.get(DISK_SHARE_ACCESS_MASK);
        if (diskfileshare != null) {
            smb2FileSystemConfigBuilder.setDiskShareAccessMask(opts, validateAndGetDiskShareAccessMask(diskfileshare, log));
        }

        return opts;
    }

    private static void applyConnectionOptions(FileSystemOptions opts, Map<String, String> options, String scheme) {
        if (VFSConstants.SCHEME_SFTP.equalsIgnoreCase(scheme)) {
            SftpFileSystemConfigBuilder sftp = SftpFileSystemConfigBuilder.getInstance();
            sftp.setAvoidPermissionCheck(opts, options.getOrDefault("transport.vfs.AvoidPermissionCheck", "true"));
            ifPresent(options, "SFTPSessionTimeout",
                    value -> sftp.setSessionTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "SFTPKeepAliveCount", value -> sftp.setKeepAliveCountMax(opts, Integer.valueOf(value)));
            ifPresent(options, "SFTPConnectTimeout",
                    value -> sftp.setConnectTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "WriteStallTimeout",
                    value -> sftp.setWriteStallTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "SFTPKeyExchangeAlgorithms", value -> sftp.setKeyExchangeAlgorithm(opts, value));
            ifPresent(options, "SFTPHostKeyAlgorithms", value -> sftp.setHostKeyAlgorithms(opts, value));
            ifPresent(options, "SFTPCiphers", value -> sftp.setCiphers(opts, value));
            ifPresent(options, "SFTPMacs", value -> sftp.setMacs(opts, value));
            ifPresent(options, "SFTPPubkeyAcceptedAlgorithms", value -> sftp.setPubkeyAcceptedAlgorithms(opts, value));
        } else if (VFSConstants.SCHEME_FTP.equalsIgnoreCase(scheme) || VFSConstants.SCHEME_FTPS.equalsIgnoreCase(scheme)) {
            FtpFileSystemConfigBuilder ftp = VFSConstants.SCHEME_FTP.equalsIgnoreCase(scheme)
                    ? FtpFileSystemConfigBuilder.getInstance() : FtpsFileSystemConfigBuilder.getInstance();
            ifPresent(options, "FTPConnectTimeout",
                    value -> ftp.setConnectTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "FTPSocketTimeout",
                    value -> ftp.setSoTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "FTPDataTimeout",
                    value -> ftp.setDataTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "WriteStallTimeout",
                    value -> ftp.setWriteStallTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
        } else if (VFSConstants.SCHEME_SMB2.equalsIgnoreCase(scheme)) {
            Smb2FileSystemConfigBuilder smb = Smb2FileSystemConfigBuilder.getInstance();
            ifPresent(options, "SMB2ReadTimeout",
                    value -> smb.setReadTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "SMB2WriteTimeout",
                    value -> smb.setWriteTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "SMB2TransactTimeout",
                    value -> smb.setTransactTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
            ifPresent(options, "SMB2DfsEnabled", value -> smb.setDfsEnabled(opts, Boolean.parseBoolean(value)));
            ifPresent(options, "WriteStallTimeout",
                    value -> smb.setWriteStallTimeout(opts, Duration.ofMillis(Long.parseLong(value))));
        }
    }

    private static void ifPresent(Map<String, String> options, String key, Consumer<String> setter) {
        String value = options.get("transport.vfs." + key);
        if (value != null) {
            setter.accept(value);
        }
    }

    private static ArrayList<String> validateAndGetDiskShareAccessMask(String diskShareAccessMask, NamedLog log) {
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

    /** The text and the stack trace of the error, for a named log call, which masks and prefixes every line. */
    public static String withTrace(String text, Throwable error) {
        StringWriter trace = new StringWriter();
        error.printStackTrace(new PrintWriter(trace));
        return text + System.lineSeparator() + trace.toString().trim();
    }
}
