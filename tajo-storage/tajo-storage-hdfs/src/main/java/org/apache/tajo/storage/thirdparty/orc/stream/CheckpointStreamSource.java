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

import com.google.common.base.MoreObjects;
import org.apache.tajo.storage.thirdparty.orc.checkpoint.StreamCheckpoint;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class CheckpointStreamSource<S extends ValueStream<C>, C extends StreamCheckpoint>
        implements StreamSource<S>
{
    public static <S extends ValueStream<C>, C extends StreamCheckpoint> CheckpointStreamSource<S, C> createCheckpointStreamSource(S stream, StreamCheckpoint checkpoint)
    {
        checkNotNull(stream, "stream is null");
        checkNotNull(checkpoint, "checkpoint is null");

        Class<? extends C> checkpointType = stream.getCheckpointType();
        C verifiedCheckpoint = OrcStreamUtils.checkType(checkpoint, checkpointType, "Checkpoint");
        return new CheckpointStreamSource<S, C>(stream, verifiedCheckpoint);
    }

    private final S stream;
    private final C checkpoint;

    public CheckpointStreamSource(S stream, C checkpoint)
    {
        this.stream = checkNotNull(stream, "stream is null");
        this.checkpoint = checkNotNull(checkpoint, "checkpoint is null");
    }

    @Override
    public Class<S> getStreamType()
    {
        return (Class<S>) stream.getClass();
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        stream.seekToCheckpoint(checkpoint);
        return stream;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("stream", stream)
                .add("checkpoint", checkpoint)
                .toString();
    }
}
