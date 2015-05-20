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
package org.apache.tajo.storage.thirdparty.orc.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RowGroupIndex
{
    private final List<Integer> positions;
    private final ColumnStatistics statistics;

    public RowGroupIndex(List<Integer> positions, ColumnStatistics statistics)
    {
        this.positions = ImmutableList.copyOf(checkNotNull(positions, "positions is null"));
        this.statistics = checkNotNull(statistics, "statistics is null");
    }

    public List<Integer> getPositions()
    {
        return positions;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return statistics;
    }
}
