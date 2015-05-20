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
package org.apache.tajo.storage.thirdparty.orc.checkpoint;

import org.apache.tajo.storage.thirdparty.orc.checkpoint.Checkpoints.ColumnPositionsList;
import org.apache.tajo.storage.thirdparty.orc.metadata.CompressionKind;
import com.google.common.base.MoreObjects;

import static org.apache.tajo.storage.thirdparty.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static org.apache.tajo.storage.thirdparty.orc.checkpoint.InputStreamCheckpoint.inputStreamCheckpointToString;
import static com.google.common.base.Preconditions.checkNotNull;

public final class LongStreamV2Checkpoint
        implements LongStreamCheckpoint
{
    private final int offset;
    private final long inputStreamCheckpoint;

    public LongStreamV2Checkpoint(int offset, long inputStreamCheckpoint)
    {
        this.offset = offset;
        this.inputStreamCheckpoint = checkNotNull(inputStreamCheckpoint, "inputStreamCheckpoint is null");
    }

    public LongStreamV2Checkpoint(CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        inputStreamCheckpoint = createInputStreamCheckpoint(compressionKind, positionsList);
        offset = positionsList.nextPosition();
    }

    public int getOffset()
    {
        return offset;
    }

    public long getInputStreamCheckpoint()
    {
        return inputStreamCheckpoint;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("offset", offset)
                .add("inputStreamCheckpoint", inputStreamCheckpointToString(inputStreamCheckpoint))
                .toString();
    }
}
