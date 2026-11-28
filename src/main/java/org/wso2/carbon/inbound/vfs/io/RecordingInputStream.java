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
package org.wso2.carbon.inbound.vfs.io;

import org.wso2.carbon.inbound.vfs.poll.Generation;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

public final class RecordingInputStream extends FilterInputStream {
    private final Generation generation;
    private final long threshold;
    private final LongConsumer barrier;
    private long count;
    private boolean reached;
    private boolean closed;
    private IOException failure;
    private long elapsed;

    public RecordingInputStream(InputStream stream, Generation generation, long threshold, LongConsumer barrier) {
        super(stream);
        this.generation = generation;
        this.threshold = threshold;
        this.barrier = barrier;
    }

    public IOException failure() {
        return failure;
    }

    public long failureMillis() {
        return elapsed;
    }

    private void current() throws IOException {
        if (generation.isRetired()) {
            throw new IOException("The file generation has retired.");
        }
    }

    private void advance(int size) {
        count += size;
        if (size > 0) {
            generation.progress(size);
        }
        if (!reached && threshold >= 0 && count >= threshold) {
            reached = true;
            barrier.accept(count);
        }
    }

    private IOException record(IOException error, long start) {
        if (failure == null) {
            failure = error;
            elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        }
        return error;
    }

    @Override
    public int read() throws IOException {
        long start = System.nanoTime();
        try {
            advance(0);
            current();
            int value = in.read();
            if (value >= 0) {
                advance(1);
            }
            return value;
        } catch (IOException error) {
            throw record(error, start);
        }
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (offset < 0 || length < 0 || length > bytes.length - offset || offset > bytes.length) {
            throw new IndexOutOfBoundsException();
        }
        long start = System.nanoTime();
        try {
            advance(0);
            current();
            int limit = length;
            if (!reached && threshold > count && length > 0) {
                limit = (int) Math.min(length, threshold - count);
            }
            int size = in.read(bytes, offset, limit);
            if (size > 0) {
                advance(size);
            }
            return size;
        } catch (IOException error) {
            throw record(error, start);
        }
    }

    @Override
    public long skip(long size) throws IOException {
        byte[] buffer = new byte[(int) Math.min(Math.max(0, size), 8192)];
        long skipped = 0;
        while (skipped < size) {
            int read = read(buffer, 0, (int) Math.min(buffer.length, size - skipped));
            if (read <= 0) {
                break;
            }
            skipped += read;
        }
        return skipped;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            in.close();
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Cannot reset the file content stream.");
    }
}

