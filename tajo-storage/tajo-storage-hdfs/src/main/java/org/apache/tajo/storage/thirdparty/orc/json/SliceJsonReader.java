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
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;

import java.io.IOException;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind.*;

public class SliceJsonReader
        implements JsonMapKeyReader
{
    private final StreamDescriptor streamDescriptor;
    private final SliceDirectJsonReader directReader;
    private final SliceDictionaryJsonReader dictionaryReader;
    private JsonMapKeyReader currentReader;

    public SliceJsonReader(StreamDescriptor streamDescriptor, boolean writeBinary)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        directReader = new SliceDirectJsonReader(streamDescriptor, writeBinary);
        dictionaryReader = new SliceDictionaryJsonReader(streamDescriptor, writeBinary);
    }

    @Override
    public void readNextValueInto(JsonGenerator generator)
            throws IOException
    {
        currentReader.readNextValueInto(generator);
    }

    @Override
    public String nextValueAsMapKey()
            throws IOException
    {
        return currentReader.nextValueAsMapKey();
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        currentReader.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources,
            List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind columnEncodingKind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (columnEncodingKind == DIRECT || columnEncodingKind == DIRECT_V2 || columnEncodingKind == ColumnEncodingKind.DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (columnEncodingKind == DICTIONARY || columnEncodingKind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + columnEncodingKind);
        }

        currentReader.openStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        currentReader.openRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
