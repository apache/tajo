/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.catalog.statistics;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class StatisticsUtil {
  private static final Log LOG = LogFactory.getLog(StatisticsUtil.class);

  public static StatSet aggregateStatSet(List<StatSet> statSets) {
    StatSet aggregated = new StatSet();

    for (StatSet statSet : statSets) {
      for (Stat stat : statSet.getAllStats()) {
        if (aggregated.containStat(stat.getType())) {
          aggregated.getStat(stat.getType()).incrementBy(stat.getValue());
        } else {
          aggregated.putStat(stat);
        }
      }
    }
    return aggregated;
  }

  /**
   * Aggregate one table stats and accumulated stats, and then store it to the result stats.
   *
   * @param result It stores the aggregated stats
   * @param stats The TableStats to be aggregated
   */
  public static void aggregateTableStat(TableStats result, TableStats stats) {

    if (stats.getColumnStats().size() > 0) {
      if (result.getColumnStats().size() == 0) {
        for (int i = 0; i < stats.getColumnStats().size(); i++) {
          result.addColumnStat(stats.getColumnStats().get(i));
        }
      } else {
        // aggregate column stats for each table
        for (int i = 0; i < stats.getColumnStats().size(); i++) {
          ColumnStats cs = stats.getColumnStats().get(i);
          ColumnStats agg = result.getColumnStats().get(i);
          if (cs == null) {
            LOG.warn("ERROR: One of column stats is NULL (expected column: " + agg.getColumn() + ")");
            continue;
          }

          try {
            agg.setNumDistVals(agg.getNumDistValues() + cs.getNumDistValues());
            agg.setNumNulls(agg.getNumNulls() + cs.getNumNulls());
            if (!cs.minIsNotSet() && (agg.minIsNotSet() ||
                agg.getMinValue().compareTo(cs.getMinValue()) > 0)) {
              agg.setMinValue(cs.getMinValue());
            }
            if (!cs.maxIsNotSet() && (agg.maxIsNotSet() ||
                agg.getMaxValue().compareTo(cs.getMaxValue()) < 0)) {
              agg.setMaxValue(stats.getColumnStats().get(i).getMaxValue());
            }
          } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
          }
        }
      }
    }

    result.setNumRows(result.getNumRows() + stats.getNumRows());
    result.setNumBytes(result.getNumBytes() + stats.getNumBytes());
    result.setReadBytes(result.getReadBytes() + stats.getReadBytes());
    result.setNumBlocks(result.getNumBlocks() + stats.getNumBlocks());
    result.setNumShuffleOutputs(result.getNumShuffleOutputs() + stats.getNumShuffleOutputs());
  }

  public static TableStats aggregateTableStat(List<TableStats> tableStatses) {
    TableStats aggregated = new TableStats();

    if(tableStatses == null || tableStatses.size() == 0 || tableStatses.get(0) == null)
      return aggregated;

    ColumnStats[] css = null;
    if (tableStatses.size() > 0) {
      for (TableStats ts : tableStatses) {
        // A TableStats cannot contain any ColumnStat if there is no output.
        // So, we should consider such a condition.
        if (ts.getColumnStats().size() > 0) {
          css = new ColumnStats[ts.getColumnStats().size()];
          for (int i = 0; i < css.length; i++) {
            css[i] = new ColumnStats(ts.getColumnStats().get(i).getColumn());
          }
          break;
        }
      }
    }

    for (TableStats ts : tableStatses) {
      // if there is empty stats
      if (ts.getColumnStats().size() > 0) {
        // aggregate column stats for each table
        for (int i = 0; i < ts.getColumnStats().size(); i++) {
          ColumnStats cs = ts.getColumnStats().get(i);
          if (cs == null) {
            LOG.warn("ERROR: One of column stats is NULL (expected column: " + css[i].getColumn() + ")");
            continue;
          }
          try {
            css[i].setNumDistVals(css[i].getNumDistValues() + cs.getNumDistValues());
            css[i].setNumNulls(css[i].getNumNulls() + cs.getNumNulls());
            if (!cs.minIsNotSet() && (css[i].minIsNotSet() ||
                css[i].getMinValue().compareTo(cs.getMinValue()) > 0)) {
              css[i].setMinValue(cs.getMinValue());
            }
            if (!cs.maxIsNotSet() && (css[i].maxIsNotSet() ||
                css[i].getMaxValue().compareTo(cs.getMaxValue()) < 0)) {
              css[i].setMaxValue(ts.getColumnStats().get(i).getMaxValue());
            }
          } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
          }
        }
      }

      // aggregate table stats for each table
      aggregated.setNumRows(aggregated.getNumRows() + ts.getNumRows());
      aggregated.setNumBytes(aggregated.getNumBytes() + ts.getNumBytes());
      aggregated.setReadBytes(aggregated.getReadBytes() + ts.getReadBytes());
      aggregated.setNumBlocks(aggregated.getNumBlocks() + ts.getNumBlocks());
      aggregated.setNumShuffleOutputs(aggregated.getNumShuffleOutputs() + ts.getNumShuffleOutputs());
    }

    //aggregated.setAvgRows(aggregated.getNumRows() / tableStats.size());
    if (css != null) {
      aggregated.setColumnStats(Lists.newArrayList(css));
    }

    return aggregated;
  }
}