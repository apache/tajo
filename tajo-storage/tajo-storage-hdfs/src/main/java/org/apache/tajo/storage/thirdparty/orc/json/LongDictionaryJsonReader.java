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
package org.apache.tajo.storage.thirdparty.orc.json;

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.stream.BooleanStream;
import org.apache.tajo.storage.thirdparty.orc.stream.LongStream;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException.verifyFormat;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.*;

public class LongDictionaryJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;

    @Nullable
    private BooleanStream presentStream;
    @Nullable
    private BooleanStream inDictionaryStream;
    @Nullable
    private LongStream dataStream;

    @Nonnull
    private long[] dictionary = new long[0];

    public LongDictionaryJsonReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        generator.writeNumber(nextValue());
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        return String.valueOf(nextValue());
    }

    private long nextValue()
            throws IOException
    {
        verifyFormat(dataStream != null, "Value is not null but data stream is not present");
        long value = dataStream.next();
        if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
            value = dictionary[((int) value)];
        }
        return value;
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        // skip non-null values
        if (inDictionaryStream != null) {
            inDictionaryStream.skip(skipSize);
        }
        if (skipSize > 0) {
            verifyFormat(dataStream != null, "Value is not null but data stream is not present");
            dataStream.skip(skipSize);
        }
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        int dictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        if (dictionarySize > 0) {
            if (dictionary.length < dictionarySize) {
                dictionary = new long[dictionarySize];
            }

            LongStream dictionaryStream = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, LongStream.class).openStream();
            verifyFormat(dictionaryStream != null, "Dictionary is not empty but data stream is not present");
            dictionaryStream.nextLongVector(dictionarySize, dictionary);
        }

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        inDictionaryStream = dataStreamSources.getStreamSource(streamDescriptor, IN_DICTIONARY, BooleanStream.class).openStream();
        dataStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class).openStream();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
