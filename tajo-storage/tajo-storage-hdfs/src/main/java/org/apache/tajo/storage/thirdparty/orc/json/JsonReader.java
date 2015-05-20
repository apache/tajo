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
import org.apache.tajo.storage.thirdparty.orc.metadata.ColumnEncoding;
import org.apache.tajo.storage.thirdparty.orc.stream.StreamSources;

import java.io.IOException;
import java.util.List;

public interface JsonReader
{
    void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException;

    void openRowGroup(StreamSources dataStreamSources)
            throws IOException;

    void readNextValueInto(JsonGenerator generator)
            throws IOException;

    void skip(int skipSize)
            throws IOException;
}
