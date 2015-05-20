/*
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
package org.apache.tajo.storage.thirdparty.orc;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import io.airlift.slice.*;
import io.airlift.slice.ChunkedSliceInput.BufferReference;
import io.airlift.slice.ChunkedSliceInput.SliceLoader;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.tajo.storage.thirdparty.orc.OrcDataSourceUtils.getDiskRangeSlice;
import static org.apache.tajo.storage.thirdparty.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractOrcDataSource
        implements OrcDataSource
{
    private final String name;
    private final long size;
    private final DataSize maxMergeDistance;
    private final DataSize maxBufferSize;
    private final DataSize streamBufferSize;
    private long readTimeNanos;

    public AbstractOrcDataSource(String name, long size, DataSize maxMergeDistance, DataSize maxBufferSize, DataSize streamBufferSize)
    {
        this.name = checkNotNull(name, "name is null");

        this.size = size;
        checkArgument(size >= 0, "size is negative");

        this.maxMergeDistance = checkNotNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxBufferSize = checkNotNull(maxBufferSize, "maxBufferSize is null");
        this.streamBufferSize = checkNotNull(streamBufferSize, "streamBufferSize is null");
    }

    protected abstract void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException;

    @Override
    public final long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public final long getSize()
    {
        return size;
    }

    @Override
    public final void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public final void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();

        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos += System.nanoTime() - start;
    }

    @Override
    public final <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        checkNotNull(diskRanges, "diskRanges is null");

        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        //
        // Note: this code does not use the Java 8 stream APIs to avoid any extra object allocation
        //

        // split disk ranges into "big" and "small"
        long maxReadSizeBytes = maxBufferSize.toBytes();
        ImmutableMap.Builder<K, DiskRange> smallRangesBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<K, DiskRange> largeRangesBuilder = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            if (entry.getValue().getLength() <= maxReadSizeBytes) {
                smallRangesBuilder.put(entry);
            }
            else {
                largeRangesBuilder.put(entry);
            }
        }
        Map<K, DiskRange> smallRanges = smallRangesBuilder.build();
        Map<K, DiskRange> largeRanges = largeRangesBuilder.build();

        // read ranges
        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        slices.putAll(readSmallDiskRanges(smallRanges));
        slices.putAll(readLargeDiskRanges(largeRanges));

        return slices.build();
    }

    private <K> Map<K, FixedLengthSliceInput> readSmallDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        Iterable<DiskRange> mergedRanges = mergeAdjacentDiskRanges(diskRanges.values(), maxMergeDistance, maxBufferSize);

        // read ranges
        Map<DiskRange, byte[]> buffers = new LinkedHashMap<DiskRange, byte[]>();
        for (DiskRange mergedRange : mergedRanges) {
            // read full range in one request
            byte[] buffer = new byte[mergedRange.getLength()];
            readFully(mergedRange.getOffset(), buffer);
            buffers.put(mergedRange, buffer);
        }

        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            slices.put(entry.getKey(), getDiskRangeSlice(entry.getValue(), buffers).getInput());
        }
        return slices.build();
    }

    private <K> Map<K, FixedLengthSliceInput> readLargeDiskRanges(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        if (diskRanges.isEmpty()) {
            return ImmutableMap.of();
        }

        ImmutableMap.Builder<K, FixedLengthSliceInput> slices = ImmutableMap.builder();
        for (Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            ChunkedSliceInput sliceInput = new ChunkedSliceInput(new HdfsSliceLoader(entry.getValue()), Ints.checkedCast(streamBufferSize.toBytes()));
            slices.put(entry.getKey(), sliceInput);
        }
        return slices.build();
    }

    @Override
    public final String toString()
    {
        return name;
    }

    private class HdfsSliceLoader
            implements SliceLoader<SliceBufferReference>
    {
        private final DiskRange diskRange;

        public HdfsSliceLoader(DiskRange diskRange)
        {
            this.diskRange = diskRange;
        }

        @Override
        public SliceBufferReference createBuffer(int bufferSize)
        {
            return new SliceBufferReference(bufferSize);
        }

        @Override
        public long getSize()
        {
            return diskRange.getLength();
        }

        @Override
        public void load(long position, SliceBufferReference bufferReference, int length)
        {
            try {
                readFully(diskRange.getOffset() + position, bufferReference.getBuffer(), 0, length);
            }
            catch (IOException e) {
                new RuntimeIOException(e);
            }
        }

        @Override
        public void close()
        {
        }
    }

    private static class SliceBufferReference
            implements BufferReference
    {
        private final byte[] buffer;
        private final Slice slice;

        public SliceBufferReference(int bufferSize)
        {
            this.buffer = new byte[bufferSize];
            this.slice = Slices.wrappedBuffer(buffer);
        }

        public byte[] getBuffer()
        {
            return buffer;
        }

        @Override
        public Slice getSlice()
        {
            return slice;
        }
    }
}
