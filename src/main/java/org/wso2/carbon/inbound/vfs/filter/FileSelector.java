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

package org.wso2.carbon.inbound.vfs.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.inbound.vfs.VFSConfig;
import org.wso2.vfs.shared.log.NamedLog;
import org.wso2.vfs.shared.log.UriMask;

import java.util.Set;

public final class FileSelector {
    private static final Log LOG = LogFactory.getLog(FileSelector.class);
    private final FileNameFilter names;
    private final SizeFilter size;
    private final AgeFilter age;
    private final boolean ignoreEmpty;

    public FileSelector(VFSConfig config, String endpoint, NamedLog log) {
        names = new FileNameFilter(config.getFileNamePattern());
        String prefix = "[inbound:" + UriMask.mask(endpoint).replaceAll("\\R", " ") + "] ";
        size = new SizeFilter(config.getFileSizeLimit(),
                message -> LOG.info(prefix + UriMask.mask(message).replaceAll("\\R", " ")));
        age = new AgeFilter(config.getMinimumAge(), config.getMaximumAge(), log::warn);
        ignoreEmpty = config.getCheckSizeIgnoreEmpty();
    }

    public boolean accept(String uri, String name, long bytes, long modified, long now) {
        return names.accept(name) && size.accept(uri, bytes) && age.accept(uri, modified, now)
                && (!ignoreEmpty || bytes != 0);
    }

    public void retain(Set<String> present) {
        size.retain(present);
        age.retain(present);
    }
}
