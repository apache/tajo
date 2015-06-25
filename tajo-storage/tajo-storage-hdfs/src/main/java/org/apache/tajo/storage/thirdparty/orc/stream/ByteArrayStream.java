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
package org.apache.tajo.storage.thirdparty.orc.stream;

import org.apache.tajo.storage.thirdparty.orc.checkpoint.ByteArrayStreamCheckpoint;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.tajo.storage.thirdparty.orc.stream.OrcStreamUtils.readFully;
import static org.apache.tajo.storage.thirdparty.orc.stream.OrcStreamUtils.skipFully;

public class ByteArrayStream
        implements ValueStream<ByteArrayStreamCheckpoint>
{
    private final OrcInputStream inputStream;

    public ByteArrayStream(OrcInputStream inputStream)
    {
        this.inputStream = checkNotNull(inputStream, "inputStream is null");
    }

    public byte[] next(int length)
            throws IOException
    {
        byte[] data = new byte[length];
        readFully(inputStream, data, 0, length);
        return data;
    }

    public void next(int length, byte[] data)
            throws IOException
    {
        readFully(inputStream, data, 0, length);
    }

    @Override
    public Class<ByteArrayStreamCheckpoint> getCheckpointType()
    {
        return ByteArrayStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(ByteArrayStreamCheckpoint checkpoint)
            throws IOException
    {
        inputStream.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        skipFully(inputStream, skipSize);
    }
}
