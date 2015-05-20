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
package org.apache.tajo.storage.thirdparty.orc;

import com.google.common.primitives.Ints;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class DiskRange
{
    private final long offset;
    private final int length;

    public DiskRange(long offset, int length)
    {
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length >= 0, "length is negative");

        this.offset = offset;
        this.length = length;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public long getEnd()
    {
        return offset + length;
    }

    public boolean contains(DiskRange diskRange)
    {
        return offset <= diskRange.getOffset() && diskRange.getEnd() <= getEnd();
    }

    /**
     * Returns the minimal DiskRange that encloses both this DiskRange
     * and otherDiskRange. If there was a gap between the ranges the
     * new range will cover that gap.
     */
    public DiskRange span(DiskRange otherDiskRange)
    {
        checkNotNull(otherDiskRange, "otherDiskRange is null");
        long start = Math.min(this.offset, otherDiskRange.getOffset());
        long end = Math.max(getEnd(), otherDiskRange.getEnd());
        return new DiskRange(start, Ints.checkedCast(end - start));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("length", length)
                .toString();
    }
}
