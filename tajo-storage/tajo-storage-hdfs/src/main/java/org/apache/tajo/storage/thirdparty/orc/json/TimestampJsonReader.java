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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.OrcCorruptionException.verifyFormat;
import static org.apache.tajo.storage.thirdparty.orc.metadata.Stream.StreamKind.*;
import static org.apache.tajo.storage.thirdparty.orc.reader.TimestampStreamReader.decodeTimestamp;

public class TimestampJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;

    private final long baseTimestampInSeconds;

    @Nullable
    private BooleanStream presentStream;

    @Nullable
    private LongStream secondsStream;

    @Nullable
    private LongStream nanosStream;

    public TimestampJsonReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, checkNotNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / 1000;
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            generator.writeNull();
            return;
        }

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        generator.writeNumber(timestamp);
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        if (presentStream != null && !presentStream.nextBit()) {
            return null;
        }

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

        long timestamp = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
        return String.valueOf(timestamp);
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        // skip nulls
        if (presentStream != null) {
            skipSize = presentStream.countBitsSet(skipSize);
        }

        if (skipSize == 0) {
            return;
        }

        verifyFormat(secondsStream != null, "Value is not null but seconds stream is not present");
        verifyFormat(nanosStream != null, "Value is not null but nanos stream is not present");

        // skip non-null values
        secondsStream.skip(skipSize);
        nanosStream.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStream = null;
        secondsStream = null;
        nanosStream = null;
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStream = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class).openStream();
        secondsStream = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class).openStream();
        nanosStream = dataStreamSources.getStreamSource(streamDescriptor, SECONDARY, LongStream.class).openStream();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
