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

import org.apache.axis2.description.Parameter;
import org.apache.axis2.description.TransportInDescription;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.commons.crypto.CryptoUtil;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2SynapseEnvironment;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.org.apache.commons.vfs2.provider.ftp.FtpFileType;
import org.wso2.securevault.commons.MiscellaneousUtil;
import org.wso2.vfs.shared.failstate.FailStateStore;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.RepeatLimiter;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.wso2.carbon.inbound.vfs.VFSConstants.APPEND;
import static org.wso2.carbon.inbound.vfs.VFSConstants.CLUSTER_AWARE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DEFAULT_FAILED_RECORDS_FILE_DESTINATION;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DEFAULT_FAILED_RECORDS_FILE_NAME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DEFAULT_MAX_RETRY_COUNT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DEFAULT_RECONNECT_TIMEOUT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.DEFAULT_RESPONSE_FILE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FILE_SORT_ORDER;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FILE_SORT_PARAM;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FILE_TYPE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FILE_TYPE_PREFIX;
import static org.wso2.carbon.inbound.vfs.VFSConstants.FORCE_CREATE_FOLDER;
import static org.wso2.carbon.inbound.vfs.VFSConstants.IS_MOUNTED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.MAX_RETRY_COUNT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.RECONNECT_TIMEOUT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.REPLY_FILE_NAME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.REPLY_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.SCHEME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.SFTP_PREFIX;
import static org.wso2.carbon.inbound.vfs.VFSConstants.STREAMING;
import static org.wso2.carbon.inbound.vfs.VFSConstants.SUBFOLDER_TIMESTAMP;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_BUILD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FAIL_ON_ERROR_CODE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_MASK_HEADER_PASSWORD;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_CHECK_SIZE_IGNORE_EMPTY;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_CHECK_SIZE_INTERVAL;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_DISTRIBUTED_LOCK;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_DISTRIBUTED_LOCK_TIMEOUT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_DESTINATION;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FAILED_RECORDS_FILE_NAME;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_ACTION_AFTER_ERRORS;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_ACTION_AFTER_FAILURE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_ACTION_AFTER_PROCESS;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_CONTENT_TYPE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_COUNT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_NAME_PATTERN;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_FILE_URI;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_INTERVAL;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_LOCKING;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_LOCKING_DISABLED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MAXIMUM_AGE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MINIMUM_AGE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_ERRORS;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_FAILURE;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_RESOLVEHOST_DYNAMICALLY;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_SIZE_LIMIT;
import static org.wso2.carbon.inbound.vfs.VFSConstants.UPDATE_LAST_MODIFIED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.WAIT_TIME_BEFORE_READ;

public final class VFSConfig {

    public static final int DELETE = 0;
    public static final int MOVE = 1;
    public static final int NONE = 2;

    private static final Log log = LogFactory.getLog(VFSConfig.class);
    private static final String PREFIX = "transport.vfs.";
    private static final Pattern VAULT = Pattern.compile("\\{wso2:vault-decrypt\\('(.*?)'\\)\\}");
    private static final String STALE_AGE = PREFIX + "LockStaleAge";

    private final NamedLog diagnostics;
    private final String fileURI;
    private final String replyFileURI;
    private final String replyFileName;
    private final String contentType;
    private final String fileNamePattern;
    private final boolean streaming;
    private final boolean build;
    private final boolean maskHeaderPassword;
    private final boolean failOnErrorCode;
    private final boolean append;
    private final Integer fileProcessingCount;
    private final Integer fileProcessingInterval;
    private final String fileSortParam;
    private final boolean fileSortAscending;
    private final int workerPoolSize;
    private final double fileSizeLimit;
    private final Long minimumAge;
    private final Long maximumAge;
    private final long checkSizeInterval;
    private final boolean checkSizeIgnoreEmpty;
    private final int maxReadTransportErrors;
    private final int actionAfterProcess;
    private final int actionAfterFailure;
    private final String moveAfterProcess;
    private final String moveAfterFailure;
    private final String moveAfterFailedMove;
    private final SimpleDateFormat moveTimestamp;
    private final SimpleDateFormat subfolderTimestamp;
    private final boolean updateLastModified;
    private final String tempFolderName;
    private final String tempFileSuffix;
    private final int maxRetryCount;
    private final long reconnectTimeout;
    private final long stopTimeout;
    private final boolean fileLocking;
    private final boolean autoLockRelease;
    private final long lockStaleAge;
    private final long retryMillis;
    private final int flagAfter;
    private final File oldList;
    private final Map<String, Map<String, String>> options;
    private final Set<String> deprecated;
    // Whether a boolean parameter is one of the released inbound or the transport, or new in this inbound.
    private static final boolean RELEASED = true;
    private static final boolean NEW = false;

    public VFSConfig(Properties properties) {
        this(properties, null, Boolean.parseBoolean(properties.getProperty("sequential", "true")));
    }

    VFSConfig(Properties properties, SynapseEnvironment environment, boolean sequential) {
        this(properties, environment, sequential, "file");
    }

    VFSConfig(Properties properties, SynapseEnvironment environment, boolean sequential, String endpoint) {
        diagnostics = NamedLog.inbound(log, endpoint, new RepeatLimiter());
        Properties p = decrypt(properties, environment);
        fileURI = uri(p, TRANSPORT_FILE_FILE_URI, true);
        replyFileURI = uri(p, REPLY_FILE_URI, false);
        replyFileName = p.getProperty(REPLY_FILE_NAME, DEFAULT_RESPONSE_FILE);
        contentType = p.getProperty(TRANSPORT_FILE_CONTENT_TYPE);
        fileNamePattern = pattern(p.getProperty(TRANSPORT_FILE_FILE_NAME_PATTERN));
        streaming = flag(p, STREAMING, false, RELEASED);
        build = flag(p, TRANSPORT_BUILD, false, RELEASED);
        maskHeaderPassword = flag(p, TRANSPORT_MASK_HEADER_PASSWORD, false, NEW);
        failOnErrorCode = flag(p, TRANSPORT_FAIL_ON_ERROR_CODE, false, NEW);
        append = flag(p, APPEND, false, RELEASED);
        fileProcessingCount = integer(p, TRANSPORT_FILE_COUNT, null, 0);
        fileProcessingInterval = integer(p, TRANSPORT_FILE_INTERVAL, null, 0);
        fileSortParam = p.getProperty(FILE_SORT_PARAM);
        fileSortAscending = flag(p, FILE_SORT_ORDER, true, RELEASED);
        workerPoolSize = sequential ? 4 : integer(p, PREFIX + "WorkerPoolSize", 4, 1);
        fileSizeLimit = sizeLimit(p);
        if (fileSizeLimit == 0) {
            diagnostics.warn(TRANSPORT_FILE_SIZE_LIMIT + " is 0, so only empty files are taken, as in the transport."
                    + " Use -1 for no limit.");
        }
        minimumAge = number(p, TRANSPORT_FILE_MINIMUM_AGE, null, 0);
        maximumAge = number(p, TRANSPORT_FILE_MAXIMUM_AGE, null, 0);
        checkSizeInterval = number(p, TRANSPORT_CHECK_SIZE_INTERVAL, 0L, 0);
        checkSizeIgnoreEmpty = flag(p, TRANSPORT_CHECK_SIZE_IGNORE_EMPTY, false, RELEASED);
        maxReadTransportErrors = integer(p, PREFIX + "MaxReadTransportErrors", 3, 1);
        actionAfterProcess = action(p, TRANSPORT_FILE_ACTION_AFTER_PROCESS);
        actionAfterFailure = action(p, TRANSPORT_FILE_ACTION_AFTER_FAILURE);
        moveTarget(p, TRANSPORT_FILE_ACTION_AFTER_PROCESS, actionAfterProcess, TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        moveTarget(p, TRANSPORT_FILE_ACTION_AFTER_FAILURE, actionAfterFailure, TRANSPORT_FILE_MOVE_AFTER_FAILURE);
        moveAfterProcess = uri(p, TRANSPORT_FILE_MOVE_AFTER_PROCESS, actionAfterProcess == MOVE);
        moveAfterFailure = uri(p, TRANSPORT_FILE_MOVE_AFTER_FAILURE, actionAfterFailure == MOVE);
        moveAfterFailedMove = uri(p, TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, false);
        moveTimestamp = datePattern(p, TRANSPORT_FILE_MOVE_TIMESTAMP_FORMAT);
        subfolderTimestamp = datePattern(p, SUBFOLDER_TIMESTAMP);
        updateLastModified = flag(p, UPDATE_LAST_MODIFIED, true, RELEASED);
        tempFolderName = tempName(p, PREFIX + "TempFolderName", false);
        tempFileSuffix = tempName(p, PREFIX + "TempFileSuffix", true);
        maxRetryCount = integer(p, MAX_RETRY_COUNT, DEFAULT_MAX_RETRY_COUNT, 0);
        reconnectTimeout = timeout(p, RECONNECT_TIMEOUT, DEFAULT_RECONNECT_TIMEOUT, true);
        stopTimeout = timeout(p, PREFIX + "StopTimeout", 60000, false);
        fileLocking = !TRANSPORT_FILE_LOCKING_DISABLED.equalsIgnoreCase(p.getProperty(TRANSPORT_FILE_LOCKING));
        autoLockRelease = flag(p, TRANSPORT_AUTO_LOCK_RELEASE, true, RELEASED);
        lockStaleAge = staleAge(p);
        retryMillis = number(p, TRANSPORT_FAILED_RECORD_NEXT_RETRY_DURATION, 30000L, 0);
        flagAfter = integer(p, PREFIX + "FailedRecordFlagAfterRetries", 12, 1);
        // The old writer concatenated these strings, including an empty or missing trailing separator.
        String oldDestination = p.getProperty(TRANSPORT_FAILED_RECORDS_FILE_DESTINATION,
                DEFAULT_FAILED_RECORDS_FILE_DESTINATION);
        String oldName = p.getProperty(TRANSPORT_FAILED_RECORDS_FILE_NAME, DEFAULT_FAILED_RECORDS_FILE_NAME);
        oldList = new File(oldDestination + oldName);
        Set<String> explicit = new LinkedHashSet<>();
        for (String key : new String[] { TRANSPORT_FAILED_RECORDS_FILE_NAME, TRANSPORT_FAILED_RECORDS_FILE_DESTINATION,
                TRANSPORT_FAILED_RECORD_TIMESTAMP_FORMAT }) {
            if (properties.containsKey(key)) {
                explicit.add(key);
            }
        }
        deprecated = Collections.unmodifiableSet(explicit);
        options = connectionOptions(p);
        ignored(p, TRANSPORT_FILE_RESOLVEHOST_DYNAMICALLY, "Every new connection already resolves the host name.");
        ignored(p, WAIT_TIME_BEFORE_READ, "Use transport.vfs.MinimumAge.");
        ignored(p, TRANSPORT_DISTRIBUTED_LOCK, "Use transport.vfs.LockStaleAge.");
        ignored(p, TRANSPORT_DISTRIBUTED_LOCK_TIMEOUT, "Use transport.vfs.LockStaleAge.");
        ignored(p, TRANSPORT_FILE_ACTION_AFTER_ERRORS, "");
        ignored(p, TRANSPORT_FILE_MOVE_AFTER_ERRORS, "");
        ignored(p, CLUSTER_AWARE, "");
    }

    private static Properties decrypt(Properties input, SynapseEnvironment environment) {
        Properties copy = new Properties();
        CryptoUtil crypto = null;
        for (String key : input.stringPropertyNames()) {
            String value = input.getProperty(key);
            Matcher match = VAULT.matcher(value);
            if (match.find()) {
                try {
                    if (crypto == null) {
                        crypto = receiverCrypto(environment, key);
                    }
                    StringBuffer result = new StringBuffer();
                    do {
                        byte[] plain = crypto.decrypt(match.group(1).getBytes(StandardCharsets.UTF_8));
                        // The receiver helper returns no bytes on a padding or ciphertext failure.
                        if (plain == null || plain.length == 0) {
                            throw invalid(key);
                        }
                        match.appendReplacement(result, Matcher.quoteReplacement(new String(plain,
                                StandardCharsets.UTF_8)));
                    } while (match.find());
                    value = match.appendTail(result).toString();
                } catch (Exception e) {
                    throw new IllegalArgumentException("Cannot decrypt " + key
                            + ". Check the vfs receiver keystore settings.");
                }
            }
            copy.setProperty(key, value);
        }
        return copy;
    }

    private static CryptoUtil receiverCrypto(SynapseEnvironment environment, String key) throws Exception {
        if (!(environment instanceof Axis2SynapseEnvironment)) {
            throw invalid(key);
        }
        AxisConfiguration axis = ((Axis2SynapseEnvironment) environment).getAxis2ConfigurationContext()
                .getAxisConfiguration();
        TransportInDescription receiver = axis.getTransportIn("vfs");
        if (receiver == null) {
            throw invalid(key);
        }
        Properties settings = new Properties();
        for (Parameter parameter : receiver.getParameters()) {
            if (parameter.getValue() != null) {
                String value = parameter.getParameterElement() == null ? parameter.getValue().toString()
                        : MiscellaneousUtil.resolve(parameter.getParameterElement(), axis.getSecretResolver());
                settings.setProperty(parameter.getName(), value);
            }
        }
        CryptoUtil crypto = new CryptoUtil(settings);
        if (!crypto.isInitialized()) {
            throw invalid(key);
        }
        return crypto;
    }

    private static IllegalArgumentException invalid(String key) {
        return new IllegalArgumentException("Invalid value for " + key + ".");
    }

    private static String uri(Properties p, String key, boolean required) {
        String value = p.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            if (required) {
                throw invalid(key);
            }
            return null;
        }
        String path = Utils.stripVfsSchemeIfPresent(value);
        try {
            UriParser.checkUriEncoding(path);
            String location = Utils.extractPath(path);
            if (!new File(location).isAbsolute() && !windowsDrive(location)) {
                validateUriLocation(location.replace('\\', '/'), key);
            }
            return value;
        } catch (URISyntaxException | FileSystemException e) {
            throw invalid(key);
        }
    }

    private static void validateUriLocation(String location, String key) throws URISyntaxException {
        String scheme = UriParser.extractScheme(location);
        if (scheme == null || scheme.isEmpty()) {
            throw invalid(key);
        }
        String rest = location.substring(scheme.length() + 1);
        if ("file".equalsIgnoreCase(scheme)) {
            if ((!rest.startsWith("/") && !windowsDrive(rest)) || "//".equals(rest)) {
                throw invalid(key);
            }
            return;
        }
        if (!rest.startsWith("//")) {
            throw invalid(key);
        }
        // VFS accepts literal brackets in a path and underscores in a server name.
        // Check only the authority. Keep the configured path and its escapes unchanged.
        int end = rest.indexOf('/', 2);
        String authority = rest.substring(2, end < 0 ? rest.length() : end);
        // User and password are not checked: VFS takes characters there that a java.net.URI refuses.
        String checkedAuthority = authority.substring(authority.lastIndexOf('@') + 1).replace('_', 'x')
                .replace(" ", "%20");
        URI server = new URI(scheme + "://" + checkedAuthority).parseServerAuthority();
        if (server.getHost() == null || server.getRawFragment() != null || authority.endsWith(":")
                || server.getPort() > 65535) {
            throw invalid(key);
        }
    }

    private static boolean windowsDrive(String path) {
        return path.length() >= 3 && Character.isLetter(path.charAt(0)) && path.charAt(1) == ':'
                && (path.charAt(2) == '/' || path.charAt(2) == '\\');
    }

    private static String pattern(String value) {
        if (value != null) {
            try {
                Pattern.compile(value);
            } catch (IllegalArgumentException e) {
                throw invalid(TRANSPORT_FILE_FILE_NAME_PATTERN);
            }
        }
        return value;
    }

    private static String tempName(Properties p, String key, boolean suffix) {
        String value = p.getProperty(key, ".part");
        boolean valid = suffix ? value.length() > 1 && value.startsWith(".") && !value.endsWith(".lock")
                && !value.endsWith(".fail") && !value.endsWith(".next")
                : !value.isEmpty() && !value.equals(".") && !value.equals("..");
        if (!valid || value.indexOf('/') >= 0 || value.indexOf('\\') >= 0) {
            throw new IllegalArgumentException("Invalid value for " + key + ". It must " + (suffix
                    ? "start with a dot, have at least one character after it, hold no path separator and not end"
                    + " in .lock, .fail or .next, which the inbound uses for its own files."
                    : "be a folder name with no path separator."));
        }
        return value;
    }

    private static SimpleDateFormat datePattern(Properties p, String key) {
        String value = p.getProperty(key);
        if (value != null) {
            try {
                return new SimpleDateFormat(value);
            } catch (IllegalArgumentException e) {
                throw invalid(key);
            }
        }
        return null;
    }

    private static Long number(Properties p, String key, Long fallback, long min) {
        String value = p.getProperty(key);
        if (value == null || value.isEmpty() && (TRANSPORT_CHECK_SIZE_INTERVAL.equals(key)
                || TRANSPORT_FILE_MINIMUM_AGE.equals(key) || TRANSPORT_FILE_MAXIMUM_AGE.equals(key)
                || STALE_AGE.equals(key) || TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL.equals(key))) {
            return fallback;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed >= min) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            throw invalid(key);
        }
        throw invalid(key);
    }

    private static Integer integer(Properties p, String key, Integer fallback, int min) {
        Long value = number(p, key, fallback == null ? null : fallback.longValue(), min);
        if (value == null) {
            return null;
        }
        if (value > Integer.MAX_VALUE) {
            throw invalid(key);
        }
        return value.intValue();
    }

    private static double sizeLimit(Properties p) {
        try {
            double value = Double.parseDouble(p.getProperty(TRANSPORT_FILE_SIZE_LIMIT, "-1"));
            if (Double.isFinite(value) && (value == -1 || value >= 0)) {
                return value;
            }
        } catch (NumberFormatException e) {
            throw invalid(TRANSPORT_FILE_SIZE_LIMIT);
        }
        throw invalid(TRANSPORT_FILE_SIZE_LIMIT);
    }

    private boolean flag(Properties p, String key, boolean fallback, boolean released) {
        return flag(key, p.getProperty(key), fallback, released);
    }

    // A boolean that the released inbound or the transport already had reads any other text as false, as there, with
    // a warning. A boolean that is new in this inbound refuses it, because no endpoint of before can carry it.
    private boolean flag(String key, String value, boolean fallback, boolean released) {
        if (value == null) {
            return fallback;
        }
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return Boolean.parseBoolean(value);
        }
        if (!released) {
            throw invalid(key);
        }
        diagnostics.warn(key + " has the value '" + value + "', which is not true or false. It is read as false.");
        return false;
    }

    // The released inbound and the transport took any other text as DELETE.
    private int action(Properties p, String key) {
        String value = p.getProperty(key, "DELETE");
        if ("MOVE".equalsIgnoreCase(value)) {
            return MOVE;
        }
        if ("NONE".equalsIgnoreCase(value)) {
            return NONE;
        }
        if (!"DELETE".equalsIgnoreCase(value)) {
            diagnostics.warn(key + " has the value '" + value + "', which is not MOVE, DELETE or NONE."
                    + " DELETE is taken.");
        }
        return DELETE;
    }

    // MOVE with no target fails the deploy. The released inbound failed each file after the flow and left it, and the
    // transport deleted it. Neither comes back.
    private static void moveTarget(Properties p, String action, int value, String target) {
        String uri = p.getProperty(target);
        if (value == MOVE && (uri == null || uri.trim().isEmpty())) {
            throw new IllegalArgumentException(action + " is MOVE and " + target + " is not set. Set " + target
                    + " or change " + action + ".");
        }
    }

    private long timeout(Properties p, String key, long fallback, boolean reconnect) {
        String value = p.getProperty(key);
        if (value == null) {
            return fallback;
        }
        boolean millis = value.endsWith("ms");
        boolean seconds = !millis && value.endsWith("s");
        String digits = value.substring(0, value.length() - (millis ? 2 : seconds ? 1 : 0));
        try {
            long amount = Long.parseLong(digits);
            if (amount < 0) {
                throw invalid(key);
            }
            boolean bare = !millis && !seconds;
            boolean asSeconds = seconds || bare && (!reconnect || amount < 1000);
            long result = asSeconds ? Math.multiplyExact(amount, 1000) : amount;
            if (bare && reconnect) {
                diagnostics.warn(key + " " + amount + " was read as " + amount + (asSeconds ? " s." : " ms.")
                        + " Specify a unit.");
            }
            return result;
        } catch (NumberFormatException | ArithmeticException e) {
            throw invalid(key);
        }
    }

    private long staleAge(Properties p) {
        boolean sameNode = flag(p, TRANSPORT_AUTO_LOCK_RELEASE_SAME_NODE, true, RELEASED);
        Long age = number(p, STALE_AGE, null, -1);
        if (age != null) {
            ignored(p, TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL, STALE_AGE + " takes precedence.");
            if (age >= 0 && age < 60000) {
                diagnostics.warn("The lock stale age was raised to 60000 ms.");
                return 60000;
            }
            return age;
        }
        Long old = number(p, TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL, null, -1);
        if (old == null || old == -1) {
            return old == null ? 900000 : -1;
        }
        long effective = Math.max(old, sameNode ? 900000 : 60000);
        diagnostics.warn(TRANSPORT_AUTO_LOCK_RELEASE_INTERVAL + " " + old + " gives a stale age of " + effective + " ms."
                + (sameNode ? "" : " A shorter stale age increases the risk of duplicate processing."));
        return effective;
    }

    private void ignored(Properties p, String key, String reason) {
        String value = p.getProperty(key);
        if (value != null && !value.isEmpty()) {
            diagnostics.warn(key + " is ignored." + (reason.isEmpty() ? "" : " " + reason));
        }
    }

    private Map<String, Map<String, String>> connectionOptions(Properties p) {
        Map<String, String> source = query(fileURI, TRANSPORT_FILE_FILE_URI);
        Map<String, String> endpoint = new HashMap<>(source);
        for (String name : p.stringPropertyNames()) {
            if (isConnectionOption(name)) {
                String value = p.getProperty(name);
                if (value.isEmpty() && isOptionalTimeoutField(name)) {
                    continue;
                }
                endpoint.put(optionKey(name), value);
            }
        }
        Map<String, Map<String, String>> result = new HashMap<>();
        addOptions(result, TRANSPORT_FILE_FILE_URI, fileURI, endpoint, false);
        addOptions(result, TRANSPORT_FILE_MOVE_AFTER_PROCESS, moveAfterProcess, endpoint, true);
        addOptions(result, TRANSPORT_FILE_MOVE_AFTER_FAILURE, moveAfterFailure, endpoint, true);
        addOptions(result, TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE, moveAfterFailedMove, endpoint, true);
        addOptions(result, REPLY_FILE_URI, replyFileURI, endpoint, true);
        return Collections.unmodifiableMap(result);
    }

    private static boolean isOptionalTimeoutField(String key) {
        return (PREFIX + "FTPConnectTimeout").equals(key) || (PREFIX + "FTPSocketTimeout").equals(key)
                || (PREFIX + "SFTPConnectTimeout").equals(key) || (PREFIX + "SFTPSessionTimeout").equals(key);
    }

    private static boolean isConnectionOption(String key) {
        return key.startsWith(PREFIX + "SFTP") || key.startsWith(PREFIX + "FTP")
                || key.startsWith(PREFIX + "SMB2") || key.startsWith("vfs.")
                || key.equals(PREFIX + "AvoidPermissionCheck") || key.equals(PREFIX + "WriteStallTimeout")
                || key.equals(FORCE_CREATE_FOLDER) || key.equals(IS_MOUNTED)
                || key.equals(FILE_TYPE_PREFIX) || key.equals(FILE_TYPE);
    }

    private static String optionKey(String key) {
        for (VFSConstants.SFTP_FILE_OPTION option : VFSConstants.SFTP_FILE_OPTION.values()) {
            if ((SFTP_PREFIX + option).equals(key)) {
                return option.toString();
            }
        }
        return FILE_TYPE_PREFIX.equals(key) ? FILE_TYPE : key;
    }

    private static Map<String, String> query(String uri, String name) {
        try {
            Map<String, String> result = new HashMap<>();
            for (Map.Entry<String, String> entry : UriParser.extractQueryParams(uri).entrySet()) {
                result.put(optionKey(entry.getKey()), entry.getValue());
            }
            return result;
        } catch (FileSystemException e) {
            throw invalid(name);
        }
    }

    private void addOptions(Map<String, Map<String, String>> result, String name, String uri,
                                   Map<String, String> endpoint, boolean target) {
        if (uri == null) {
            return;
        }
        Map<String, String> merged = new HashMap<>(endpoint);
        if (target) {
            merged.putAll(query(uri, name));
        }
        String location = Utils.stripVfsSchemeIfPresent(uri);
        String scheme = windowsDrive(location) ? "file" : UriParser.extractScheme(location);
        merged.put(SCHEME, scheme == null ? "file" : scheme);
        for (String key : new String[] { "SFTPSessionTimeout", "SFTPConnectTimeout", "SFTPKeepAliveCount",
                "FTPConnectTimeout", "FTPSocketTimeout", "FTPDataTimeout", "SMB2ReadTimeout", "SMB2WriteTimeout",
                "SMB2TransactTimeout", "WriteStallTimeout" }) {
            String value = merged.get(PREFIX + key);
            if (value != null) {
                try {
                    if (Integer.parseInt(value) < 0) {
                        throw invalid(PREFIX + key);
                    }
                } catch (NumberFormatException e) {
                    throw invalid(PREFIX + key);
                }
            }
        }
        for (String key : new String[] { FORCE_CREATE_FOLDER, IS_MOUNTED, PREFIX + "AvoidPermissionCheck" }) {
            String value = merged.get(key);
            if (value != null) {
                // CreateFolder and IsMounted come from the released inbound, AvoidPermissionCheck from 2.2.x.
                merged.put(key, String.valueOf(flag(key, value, false, RELEASED)));
            }
        }
        String dfs = merged.get(PREFIX + "SMB2DfsEnabled");
        if (dfs != null) {
            merged.put(PREFIX + "SMB2DfsEnabled", String.valueOf(flag(PREFIX + "SMB2DfsEnabled", dfs, true, NEW)));
        }
        String type = merged.get(FILE_TYPE);
        if (type != null && ("ftp".equalsIgnoreCase(scheme) || "ftps".equalsIgnoreCase(scheme))) {
            try {
                merged.put(FILE_TYPE, FtpFileType.valueOf(type.toUpperCase(Locale.ROOT)).name());
            } catch (IllegalArgumentException e) {
                throw invalid(FILE_TYPE);
            }
        }
        result.put(name, Collections.unmodifiableMap(merged));
    }

    public Map<String, String> getOptions(String uriParameter) {
        return options.get(uriParameter);
    }

    public Map<String, String> getMoveOptions(String uri) {
        if (uri != null && uri.equals(moveAfterProcess)) {
            return options.get(TRANSPORT_FILE_MOVE_AFTER_PROCESS);
        }
        if (uri != null && uri.equals(moveAfterFailure)) {
            return options.get(TRANSPORT_FILE_MOVE_AFTER_FAILURE);
        }
        if (uri != null && uri.equals(moveAfterFailedMove)) {
            return options.get(TRANSPORT_FILE_MOVE_AFTER_FAILED_MOVE);
        }
        throw new IllegalArgumentException("Unknown move target.");
    }

    public Map<String, String> getVfsSchemeProperties() {
        return options.get(TRANSPORT_FILE_FILE_URI);
    }

    public boolean isForceCreateFolder() {
        return Boolean.parseBoolean(getVfsSchemeProperties().get(FORCE_CREATE_FOLDER));
    }

    public boolean isAvoidPermissionCheck() {
        return "sftp".equalsIgnoreCase(getVfsSchemeProperties().get(SCHEME))
                && Boolean.parseBoolean(getVfsSchemeProperties().getOrDefault(PREFIX + "AvoidPermissionCheck", "true"));
    }

    public FailStateStore.Parameters getFailStateParameters() {
        FailStateStore.Parameters parameters = new FailStateStore.Parameters();
        parameters.retryMillis = retryMillis;
        parameters.flagAfter = flagAfter;
        parameters.oldList = oldList;
        parameters.deprecated = deprecated;
        return parameters;
    }

    public String getFileURI() {
        return fileURI;
    }

    public String getReplyFileURI() {
        return replyFileURI;
    }

    public String getReplayFileName() {
        return replyFileName;
    }

    public String getContentType() {
        return contentType;
    }

    public String getFileNamePattern() {
        return fileNamePattern;
    }

    public boolean isStreaming() {
        return streaming;
    }

    public boolean isMaskHeaderPassword() {
        return maskHeaderPassword;
    }

    public boolean isFailOnErrorCode() {
        return failOnErrorCode;
    }

    public boolean isBuild() {
        return build;
    }

    public boolean isAppend() {
        return append;
    }

    public Integer getFileProcessingCount() {
        return fileProcessingCount;
    }

    public Integer getFileProcessingInterval() {
        return fileProcessingInterval;
    }

    public String getFileSortParam() {
        return fileSortParam;
    }

    public boolean isFileSortAscending() {
        return fileSortAscending;
    }

    public int getWorkerPoolSize() {
        return workerPoolSize;
    }

    public double getFileSizeLimit() {
        return fileSizeLimit;
    }

    public Long getMinimumAge() {
        return minimumAge;
    }

    public Long getMaximumAge() {
        return maximumAge;
    }

    public long getCheckSizeInterval() {
        return checkSizeInterval;
    }

    public boolean getCheckSizeIgnoreEmpty() {
        return checkSizeIgnoreEmpty;
    }

    public int getMaxReadTransportErrors() {
        return maxReadTransportErrors;
    }

    public int getActionAfterProcess() {
        return actionAfterProcess;
    }

    public int getActionAfterFailure() {
        return actionAfterFailure;
    }

    public String getMoveAfterProcess() {
        return moveAfterProcess;
    }

    public String getMoveAfterFailure() {
        return moveAfterFailure;
    }

    public String getMoveAfterMoveFailure() {
        return moveAfterFailedMove;
    }

    public String getMoveTimestampPattern() {
        return moveTimestamp == null ? null : moveTimestamp.toPattern();
    }

    public String getSubfolderTimestamp() {
        return subfolderTimestamp == null ? null : subfolderTimestamp.toPattern();
    }

    public boolean isUpdateLastModified() {
        return updateLastModified;
    }

    public String getTempFolderName() {
        return tempFolderName;
    }

    public String getTempFileSuffix() {
        return tempFileSuffix;
    }

    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    public long getReconnectTimeout() {
        return reconnectTimeout;
    }

    public long getStopTimeout() {
        return stopTimeout;
    }

    public boolean isFileLocking() {
        return fileLocking;
    }

    public boolean isAutoLockRelease() {
        return autoLockRelease;
    }

    public long getLockStaleAge() {
        return lockStaleAge;
    }

    public long getFailedRecordNextRetryDuration() {
        return retryMillis;
    }

    public int getFailedRecordFlagAfterRetries() {
        return flagAfter;
    }

    public File getOldList() {
        return oldList;
    }

    public DateFormat getMoveTimestampFormat() {
        return moveTimestamp == null ? null : (DateFormat) moveTimestamp.clone();
    }
}
