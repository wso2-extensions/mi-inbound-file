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

public final class AgeFilter {
    private final Long minimum;
    private final Long maximum;
    private final Consumer<String> warn;
    private final Set<String> unknownTime = new HashSet<>();

    public AgeFilter(Long minimum, Long maximum, Consumer<String> warn) {
        this.minimum = minimum;
        this.maximum = maximum;
        this.warn = warn;
    }

    public boolean accept(String uri, long modified, long now) {
        if (minimum == null && maximum == null) {
            return true;
        }
        if (modified == 0) {
            if (unknownTime.add(uri)) {
                warn.accept("The listing has no modified time for " + uri);
            }
            return maximum == null;
        }
        long seconds = (now - modified) / 1000L;
        return (minimum == null || seconds > minimum) && (maximum == null || seconds < maximum);
    }

    void retain(Set<String> present) {
        unknownTime.retainAll(present);
    }
}
