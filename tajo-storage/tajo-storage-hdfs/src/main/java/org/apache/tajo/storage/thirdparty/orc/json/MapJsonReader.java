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
import com.google.common.primitives.Ints;
import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.stream.BooleanStream;
import org.apache.tajo.storage.thirdparty.orc.stream.LongStream;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException.verifyFormat;
import static org.apache.tajo.storage.thirdparty.orc.json.JsonReaders.createJsonMapKeyReader;
import static org.apache.tajo.storage.thirdparty.orc.json.JsonReaders.createJsonReader;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.LENGTH;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.PRESENT;

public class MapJsonReader
        implements JsonReader
{
    private final StreamDescriptor streamDescriptor;
    private final boolean checkForNulls;

    private final JsonMapKeyReader keyReader;
    private final JsonReader valueReader;

    @Nullable
    private BooleanStream presentStream;
    @Nullable
    private LongStream lengthStream;

    public MapJsonReader(StreamDescriptor streamDescriptor, boolean checkForNulls, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.checkForNulls = checkForNulls;

        keyReader = createJsonMapKeyReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone);
        valueReader = createJsonReader(streamDescriptor.getNestedStreams().get(1), true, hiveStorageTimeZone);
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        verifyFormat(lengthStream != null, "Value is not null but length stream is not present");

        long length = lengthStream.next();
        generator.writeStartObject();
        for (int i = 0; i < length; i++) {
            String name = keyReader.nextValueAsMapKey();
            if (name == null) {
                valueReader.skip(1);
            }
            else {
                generator.writeFieldName(name);
                valueReader.readNextValueInto(generator);
            }
        }
        generator.writeEndObject();
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0)  {
            return;
        }

        verifyFormat(lengthStream != null, "Value is not null but length stream is not present");

        // skip non-null values
        long elementSkipSize = lengthStream.sum(skipSize);
        keyReader.skip(Ints.checkedCast(elementSkipSize));
        valueReader.skip(Ints.checkedCast(elementSkipSize));
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        lengthStream = null;

        keyReader.openStripe(dictionaryStreamSources, encoding);
        valueReader.openStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        if (checkForNulls) {
            presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        }
        lengthStream = dataStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class).openStream();

        keyReader.openRowGroup(dataStreamSources);
        valueReader.openRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
