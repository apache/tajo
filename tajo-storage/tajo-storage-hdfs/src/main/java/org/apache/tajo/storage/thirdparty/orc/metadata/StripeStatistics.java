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

public class StripeStatistics
{
    private final List<ColumnStatistics> columnStatistics;

    public StripeStatistics(List<ColumnStatistics> columnStatistics)
    {
        this.columnStatistics = ImmutableList.copyOf(checkNotNull(columnStatistics, "columnStatistics is null"));
    }

    public List<ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }
}
