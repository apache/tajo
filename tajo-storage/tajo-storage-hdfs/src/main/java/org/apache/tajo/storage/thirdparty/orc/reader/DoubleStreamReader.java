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
package org.apache.tajo.storage.thirdparty.orc.reader;

import org.apache.tajo.storage.thirdparty.orc.DoubleVector;
import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.stream.BooleanStream;
import org.apache.tajo.storage.thirdparty.orc.stream.DoubleStream;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSource;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException.verifyFormat;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.DATA;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.PRESENT;
import static org.apache.tajo.storage.thirdparty.orc.stream.MissingStreamSource.missingStreamSource;

public class DoubleStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    @Nonnull
    private StreamSource<DoubleStream> dataStreamSource = missingStreamSource(DoubleStream.class);
    @Nullable
    private DoubleStream dataStream;

    private boolean rowGroupOpen;

    public DoubleStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                verifyFormat(dataStream != null, "Value is not null but data stream is not present");
                dataStream.skip(readOffset);
            }
        }

        DoubleVector doubleVector = (DoubleVector) vector;
        if (presentStream == null) {
            verifyFormat(dataStream != null, "Value is not null but data stream is not present");
            Arrays.fill(doubleVector.isNull, false);
            dataStream.nextVector(nextBatchSize, doubleVector.vector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, doubleVector.isNull);
            if (nullValues != nextBatchSize) {
                verifyFormat(dataStream != null, "Value is not null but data stream is not present");
                dataStream.nextVector(nextBatchSize, doubleVector.vector, doubleVector.isNull);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanStream.class);
        dataStreamSource = missingStreamSource(DoubleStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, DoubleStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
