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

package org.wso2.carbon.inbound.vfs.processor;

import org.wso2.carbon.inbound.vfs.Utils;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.org.apache.commons.vfs2.FileSystemException;
import org.wso2.org.apache.commons.vfs2.provider.UriParser;
import org.wso2.vfs.shared.log.NamedLog;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import static org.wso2.carbon.inbound.vfs.VFSConstants.FORCE_CREATE_FOLDER;
import static org.wso2.carbon.inbound.vfs.VFSConstants.IS_MOUNTED;
import static org.wso2.carbon.inbound.vfs.VFSConstants.TRANSPORT_FILE_MOVE_AFTER_PROCESS;

/** A configured move destination shared by the endpoint workers. */
public final class MoveTargetSpec {
    private static final Pattern URI_SCHEME = Pattern.compile("^[A-Za-z][A-Za-z0-9+.\\-]*:");

    final String parameter;
    final boolean createFolder;
    final boolean mounted;
    private final String path;
    private final String query;
    private final String datePattern;
    private final String datedPrefix;
    private final String prefixPattern;

    public static MoveTargetSpec from(String parameter, String uri, VFSConfig config, NamedLog log) {
        return uri == null ? null : new MoveTargetSpec(parameter, uri, config, log);
    }

    private MoveTargetSpec(String parameter, String uri, VFSConfig config, NamedLog log) {
        this.parameter = parameter;
        String location = Utils.stripVfsSchemeIfPresent(uri);
        String folder = Utils.extractPath(location);
        query = location.substring(folder.length());
        String base = folder.endsWith("/*") || folder.endsWith("\\*") ? folder.substring(0, folder.length() - 1) : folder;
        Map<String, String> options = config.getOptions(parameter);
        createFolder = Boolean.parseBoolean(options.get(FORCE_CREATE_FOLDER));
        try {
            mounted = Boolean.parseBoolean(UriParser.extractQueryParams(uri).get(IS_MOUNTED));
        } catch (FileSystemException invalid) {
            throw new IllegalArgumentException("Invalid value for " + parameter + ".");
        }
        datePattern = TRANSPORT_FILE_MOVE_AFTER_PROCESS.equals(parameter) ? config.getSubfolderTimestamp() : null;
        // As the transport, the date is added to the configured folder with no separator. When neither side has a
        // slash, the dated folder is a sibling, so the paths of this target count from the parent folder.
        String leaf = "";
        if (datePattern != null && !base.endsWith("/") && !base.endsWith("\\")
                && !new SimpleDateFormat(datePattern).format(new Date(0)).startsWith("/")) {
            int slash = Math.max(base.lastIndexOf('/'), base.lastIndexOf('\\'));
            leaf = base.substring(slash + 1);
            base = base.substring(0, slash + 1);
        }
        path = base;
        datedPrefix = leaf;
        prefixPattern = config.getMoveTimestampPattern();
        if (datePattern != null && !createFolder) {
            log.warn("SubFolderTimestampFormat is set with CreateFolder off. The dated move folder must exist.");
        }
    }

    /** relative is the encoded relative parent from the poll listing, including its leading slash. */
    public Target target(String relative, String baseName, long millis) {
        Date now = new Date(millis);
        String dated = datePattern == null ? ""
                : (datedPrefix + new SimpleDateFormat(datePattern).format(now)).replaceFirst("^/+", "");
        String name = prefixPattern == null ? baseName : new SimpleDateFormat(prefixPattern).format(now) + baseName;
        String parent = relative.isEmpty() ? dated : append(dated, relative.substring(1));
        return location(dated, parent, append(parent, name));
    }

    /**
     * A marker stores only the path below the configured move folder. When the date continues the folder name, it
     * stores the path from the parent folder, and that path starts with the folder name.
     */
    public Target recorded(String relative, String recordedPath) {
        if (recordedPath.isEmpty() || recordedPath.startsWith("/") || recordedPath.indexOf('?') >= 0
                || recordedPath.indexOf('\\') >= 0 || hasSchemePrefix(recordedPath)
                || !recordedPath.startsWith(encodeLeadingColon(datedPrefix))) {
            throw new IllegalArgumentException("Invalid recorded move target.");
        }
        for (String segment : recordedPath.split("/", -1)) {
            try {
                String decoded = UriParser.decode(segment);
                if (decoded.isEmpty() || decoded.equals(".") || decoded.equals("..")
                        || decoded.indexOf('/') >= 0 || decoded.indexOf('\\') >= 0) {
                    throw new IllegalArgumentException("Invalid recorded move target.");
                }
            } catch (FileSystemException invalid) {
                throw new IllegalArgumentException("Invalid recorded move target.", invalid);
            }
        }
        int slash = recordedPath.lastIndexOf('/');
        String parent = slash < 0 ? "" : recordedPath.substring(0, slash);
        String suffix = relative.isEmpty() ? "" : relative.substring(1);
        String dated;
        if (suffix.isEmpty()) {
            dated = parent;
        } else if (parent.equals(suffix) || parent.equals(encodeLeadingColon(suffix))) {
            dated = "";
        } else if (parent.endsWith("/" + suffix)) {
            dated = parent.substring(0, parent.length() - relative.length());
        } else {
            throw new IllegalArgumentException("The recorded move folder does not match the relative path.");
        }
        return location(dated, parent, recordedPath);
    }

    private Target location(String dated, String parent, String file) {
        String recorded = encodeLeadingColon(file);
        return new Target(append(path, encodeLeadingColon(dated)) + query,
                append(path, encodeLeadingColon(parent)) + query, append(path, recorded) + query, recorded);
    }

    private static String encodeLeadingColon(String relative) {
        if (!hasSchemePrefix(relative)) {
            return relative;
        }
        // Preserve the encoded listing path. Only the scheme separator needs escaping.
        int colon = relative.indexOf(':');
        return relative.substring(0, colon) + "%3A" + relative.substring(colon + 1);
    }

    private static boolean hasSchemePrefix(String relative) {
        return relative.indexOf(':') >= 0 && URI_SCHEME.matcher(relative).lookingAt();
    }

    private static String append(String parent, String child) {
        if (parent.isEmpty()) {
            return child;
        }
        return child.isEmpty() || parent.endsWith("/") ? parent + child : parent + "/" + child;
    }

    public static final class Target {
        public final String folderUri;
        public final String parentUri;
        public final String fileUri;
        public final String recordedPath;

        String sibling(String name) {
            String parent = Utils.extractPath(parentUri);
            return append(parent, name) + parentUri.substring(parent.length());
        }

        private Target(String folderUri, String parentUri, String fileUri, String recordedPath) {
            this.folderUri = folderUri;
            this.parentUri = parentUri;
            this.fileUri = fileUri;
            this.recordedPath = recordedPath;
        }
    }
}
