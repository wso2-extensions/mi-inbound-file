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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public final class SizeFilter {
    private final double limit;
    private final Consumer<String> info;
    private final Set<String> reported = new HashSet<>();

    public SizeFilter(double limit, Consumer<String> info) {
        this.limit = limit;
        this.info = info;
    }

    public boolean accept(String uri, long bytes) {
        if (limit < 0 || bytes <= limit) {
            return true;
        }
        if (reported.add(uri)) {
            info.accept("File exceeds the size limit. " + uri);
        }
        return false;
    }

    void retain(Set<String> present) {
        reported.retainAll(present);
    }
}
