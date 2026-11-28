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

import org.junit.jupiter.api.Test;
import org.wso2.carbon.inbound.vfs.poll.Generation;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RecordingInputStreamTest {
    @Test
    void prefixIsMeasuredBeforeTheHoldAndARetiredReaderCannotContinue() throws Exception {
        Generation gen = mock(Generation.class);
        AtomicBoolean stopped = new AtomicBoolean();
        when(gen.isRetired()).thenAnswer(call -> stopped.get());
        ByteArrayInputStream raw = new ByteArrayInputStream(new byte[]{1, 2, 3, 4});
        AtomicLong reached = new AtomicLong(-1);
        RecordingInputStream content = new RecordingInputStream(raw, gen, 2, n -> {
            reached.set(n);
            stopped.set(true);
        });
        assertEquals(2, content.read(new byte[8]));
        assertEquals(2, reached.get());
        IOException failure = assertThrows(IOException.class, () -> content.skip(1));
        assertSame(failure, content.failure());
        assertEquals(2, raw.available());
    }

    @Test
    void remembersFirstReadFailureAndCloseRunsOnce() throws Exception {
        Generation gen = mock(Generation.class);
        InputStream raw = mock(InputStream.class);
        IOException read = new IOException("Test read failure.");
        IOException close = new IOException("Test close failure.");
        when(raw.read()).thenThrow(read);
        doThrow(close).when(raw).close();
        RecordingInputStream content = new RecordingInputStream(raw, gen, -1, n -> fail());
        assertSame(read, assertThrows(IOException.class, content::read));
        assertSame(close, assertThrows(IOException.class, content::close));
        content.close();
        verify(raw).close();
        assertSame(read, content.failure());
    }
}

