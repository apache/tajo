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

import static org.apache.tajo.storage.thirdparty.orc.checkpoint.InputStreamCheckpoint.inputStreamCheckpointToString;

public final class RowGroupDictionaryLengthStreamCheckpoint
        extends LongStreamV1Checkpoint
{
    private final int rowGroupDictionarySize;

    public RowGroupDictionaryLengthStreamCheckpoint(int rowGroupDictionarySize, int offset, long inputStreamCheckpoint)
    {
        super(offset, inputStreamCheckpoint);
        this.rowGroupDictionarySize = rowGroupDictionarySize;
    }

    public RowGroupDictionaryLengthStreamCheckpoint(CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        super(compressionKind, positionsList);
        rowGroupDictionarySize = positionsList.nextPosition();
    }

    public int getRowGroupDictionarySize()
    {
        return rowGroupDictionarySize;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("rowGroupDictionarySize", rowGroupDictionarySize)
                .add("offset", getOffset())
                .add("inputStreamCheckpoint", inputStreamCheckpointToString(getInputStreamCheckpoint()))
                .toString();
    }
}
