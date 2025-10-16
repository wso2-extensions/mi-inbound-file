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

import org.wso2.carbon.inbound.vfs.VFSConfig;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.wso2.org.apache.commons.vfs2.FileObject;
import org.wso2.org.apache.commons.vfs2.FileSystemManager;

/**
 * Class to select files based on multiple filters.
 */
public class FileSelector {

    private final List<Filter> filters;
    public FileSelector(VFSConfig config, FileSystemManager fsManager) {
        filters = new ArrayList<>();
        if (config.getFileNamePattern() != null && !config.getFileNamePattern().isEmpty()) {
            filters.add(new FileNameFilter(config));
        }
        if (config.getFileSizeLimit() >= 0) {
            filters.add(new SizeFilter(config));
        }
        if (config.getCheckSizeInterval() != null && !config.getCheckSizeInterval().isEmpty()) {
            filters.add(new SizeCheckFilter(config, fsManager));
        }
        if ((config.getMaximumAge() != null && !config.getMaximumAge().toString().isEmpty()) ||
                (config.getMinimumAge()!= null && !config.getMinimumAge().toString().isEmpty())) {
            filters.add(new AgeFilter(config));
        }
    }

    public boolean isValidFile(FileObject fileObject) {
        for (Filter filter : filters) {
            if (!filter.accept(fileObject)) {
                return false;
            }
        }
        return true;
    }
}
