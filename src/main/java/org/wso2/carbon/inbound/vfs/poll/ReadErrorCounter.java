/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.wso2.carbon.inbound.vfs.poll;

import org.wso2.vfs.shared.failstate.FileIdentity;
import org.wso2.vfs.shared.health.EndpointHealth;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ReadErrorCounter {
    // Health survives redeploy. Keep only the memberships that this endpoint reported.
    private static final Map<EndpointHealth, Flags> ENDPOINT_FLAGS = new WeakHashMap<>();

    private static final class Flags {
        final Map<String, Identity> paths = new HashMap<>();
        Object owner;
    }

    // Command and listing times can differ for the same FTP file. Only compare like sources.
    private static final class Identity {
        final FileIdentity first;
        final FileIdentity second;

        Identity(FileIdentity first, FileIdentity second) {
            this.first = first;
            this.second = second;
        }

        FileIdentity comparable(FileIdentity file) {
            return first.mtimeSource == file.mtimeSource ? first : second;
        }
    }

    private static final class Reads {
        Identity identity;
        int failures;
        boolean flagged;

        Reads(Identity identity) {
            this.identity = identity;
        }
    }

    private final int maximum;
    private final Object owner = new Object();
    private final Flags flags;
    private final ConcurrentMap<String, Reads> files = new ConcurrentHashMap<>();

    public ReadErrorCounter(int maximum) {
        this(maximum, new Flags());
    }

    public static ReadErrorCounter forEndpoint(EndpointHealth health, int maximum) {
        Flags flags;
        synchronized (ENDPOINT_FLAGS) {
            flags = ENDPOINT_FLAGS.computeIfAbsent(health, key -> new Flags());
        }
        return new ReadErrorCounter(maximum, flags);
    }

    private ReadErrorCounter(int maximum, Flags flags) {
        if (maximum < 1) {
            throw new IllegalArgumentException("The read error limit must be positive.");
        }
        this.maximum = maximum;
        this.flags = flags;
        synchronized (flags) {
            flags.paths.forEach((path, identity) -> {
                Reads reads = new Reads(identity);
                reads.flagged = true;
                files.put(path, reads);
            });
            flags.owner = owner;
        }
    }

    public void transportFailure(Generation generation, FileIdentity file, boolean listingSucceeded) {
        if (listingSucceeded) {
            synchronized (generation.lifecycle) {
                if (generation.isRetired()) {
                    return;
                }
                Reads reads = matching(generation, file);
                if (reads == null) {
                    reads = new Reads(new Identity(file, null));
                    files.put(file.uri, reads);
                }
                if (reads.failures < maximum) {
                    reads.failures++;
                }
            }
        }
    }

    public boolean capped(Generation generation, FileIdentity file) {
        if (files.isEmpty()) {
            return false;
        }
        synchronized (generation.lifecycle) {
            if (generation.isRetired()) {
                return false;
            }
            Reads reads = matching(generation, file);
            return reads != null && reads.failures >= maximum;
        }
    }

    public void flagCapped(Generation generation, FileIdentity file) {
        synchronized (generation.lifecycle) {
            if (!generation.isRetired()) {
                Reads reads = matching(generation, file);
                if (reads != null && reads.failures >= maximum && !reads.flagged) {
                    synchronized (flags) {
                        if (flags.owner == owner) {
                            generation.flag(EndpointHealth.Flag.READ_CAP, file.uri);
                            flags.paths.put(file.uri, reads.identity);
                            reads.flagged = true;
                        }
                    }
                }
            }
        }
    }

    public void goodRead(Generation generation, FileIdentity file) {
        if (!files.isEmpty()) {
            synchronized (generation.lifecycle) {
                if (!generation.isRetired()) {
                    remove(generation, file.uri);
                }
            }
        }
    }

    public void listed(Generation generation, Set<String> present) {
        if (!files.isEmpty()) {
            synchronized (generation.lifecycle) {
                if (!generation.isRetired()) {
                    for (String path : files.keySet()) {
                        if (!present.contains(path)) {
                            remove(generation, path);
                        }
                    }
                }
            }
        }
    }

    private Reads matching(Generation generation, FileIdentity file) {
        Reads reads = files.get(file.uri);
        if (reads == null) {
            return null;
        }
        Identity identity = reads.identity;
        FileIdentity previous = identity.comparable(file);
        if (identity.first.size != file.size || previous != null && previous.mtime != file.mtime) {
            remove(generation, file.uri);
            return null;
        }
        if (previous == null) {
            reads.identity = new Identity(identity.first, file);
            if (reads.flagged) {
                synchronized (flags) {
                    if (flags.owner == owner) {
                        flags.paths.put(file.uri, reads.identity);
                    }
                }
            }
        }
        return reads;
    }

    private void remove(Generation generation, String path) {
        Reads reads = files.remove(path);
        if (reads != null && reads.flagged) {
            synchronized (flags) {
                if (flags.owner == owner) {
                    generation.clearFlag(EndpointHealth.Flag.READ_CAP, path);
                    flags.paths.remove(path);
                }
            }
        }
    }
}
