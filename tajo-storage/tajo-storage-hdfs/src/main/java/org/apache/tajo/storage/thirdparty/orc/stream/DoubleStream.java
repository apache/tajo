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

import org.apache.tajo.storage.thirdparty.orc.Vector;
import org.apache.tajo.storage.thirdparty.orc.checkpoint.DoubleStreamCheckpoint;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkPositionIndex;
import static org.apache.tajo.storage.thirdparty.orc.stream.OrcStreamUtils.readFully;
import static org.apache.tajo.storage.thirdparty.orc.stream.OrcStreamUtils.skipFully;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;

public class DoubleStream
        implements ValueStream<DoubleStreamCheckpoint>
{
    private final OrcInputStream input;
    private final byte[] buffer = new byte[Vector.MAX_VECTOR_LENGTH * SIZE_OF_DOUBLE];
    private final Slice slice = Slices.wrappedBuffer(buffer);

    public DoubleStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<DoubleStreamCheckpoint> getCheckpointType()
    {
       return DoubleStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DoubleStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        long length = items * SIZE_OF_DOUBLE;
        skipFully(input, length);
    }

    public double next()
            throws IOException
    {
        readFully(input, buffer, 0, SIZE_OF_DOUBLE);
        return slice.getDouble(0);
    }

    public void nextVector(int items, double[] vector)
            throws IOException
    {
        checkPositionIndex(items, vector.length);
        checkPositionIndex(items, Vector.MAX_VECTOR_LENGTH);

        // buffer that number of values
        readFully(input, buffer, 0, items * SIZE_OF_DOUBLE);

        // copy values directly into vector
        Slices.wrappedDoubleArray(vector).setBytes(0, slice, 0, items * SIZE_OF_DOUBLE);
    }

    public void nextVector(long items, double[] vector, boolean[] isNull)
            throws IOException
    {
        // count the number of non nulls
        int notNullCount = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                notNullCount++;
            }
        }

        // buffer that umber of values
        readFully(input, buffer, 0, notNullCount * SIZE_OF_DOUBLE);

        // load them into the buffer
        int elementIndex = 0;
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = slice.getDouble(elementIndex);
                elementIndex += SIZE_OF_DOUBLE;
            }
        }
    }
}
