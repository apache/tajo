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

import org.apache.tajo.storage.thirdparty.orc.StreamDescriptor;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;

import java.io.IOException;
import java.util.List;

import static org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding.ColumnEncodingKind.*;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class LongStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final LongDirectStreamReader directReader;
    private final LongDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public LongStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        directReader = new LongDirectStreamReader(streamDescriptor);
        dictionaryReader = new LongDictionaryStreamReader(streamDescriptor);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        currentReader.prepareNextRead(batchSize);
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        currentReader.readBatch(vector);
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind kind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (kind == DIRECT || kind == DIRECT_V2 || kind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (kind == DICTIONARY) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
