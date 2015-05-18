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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.tajo.storage.thirdparty.orc.SliceVector;
import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.Vector;
import org.apache.tajo.storage.thirdparty.orc.json.JsonReader;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.stream.BooleanStream;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSource;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;
import io.airlift.slice.DynamicSliceOutput;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.json.JsonReaders.createJsonReader;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.PRESENT;
import static org.apache.tajo.storage.thirdparty.orc.stream.MissingStreamSource.missingStreamSource;

public class JsonStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final JsonReader jsonReader;

    private boolean stripeOpen;
    private boolean rowGroupOpen;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    private int readOffset;
    private int nextBatchSize;

    @Nullable
    private StreamSources dictionaryStreamSources;
    @Nullable
    private StreamSources dataStreamSources;

    private List<ColumnEncoding> encoding;

    public JsonStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.jsonReader = createJsonReader(streamDescriptor, false, hiveStorageTimeZone);
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
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }

            jsonReader.skip(readOffset);
        }

        SliceVector sliceVector = (SliceVector) vector;
        if (presentStream != null) {
            presentStream.getUnsetBits(nextBatchSize, isNullVector);
        }

        DynamicSliceOutput out = new DynamicSliceOutput(1024);
        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                out.reset();
                JsonGenerator generator = new JsonFactory().createGenerator(out);
                jsonReader.readNextValueInto(generator);
                sliceVector.vector[i] = out.copySlice();
            }
            else {
                sliceVector.vector[i] = null;
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

        if (!stripeOpen) {
            jsonReader.openStripe(dictionaryStreamSources, encoding);
        }

        jsonReader.openRowGroup(dataStreamSources);

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        this.dictionaryStreamSources = dictionaryStreamSources;
        this.dataStreamSources = null;
        this.encoding = encoding;

        presentStreamSource = missingStreamSource(BooleanStream.class);

        stripeOpen = false;
        rowGroupOpen = false;

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        this.dataStreamSources = dataStreamSources;

        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);

        rowGroupOpen = false;

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
