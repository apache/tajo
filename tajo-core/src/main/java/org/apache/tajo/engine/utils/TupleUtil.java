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

package org.apache.tajo.engine.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.VTuple;

import java.util.List;
import java.util.Map;

public class TupleUtil {
  private static final Log LOG = LogFactory.getLog(TupleUtil.class);

  /**
   * if max value is null, set ranges[last]
   * @param sortSpecs
   * @param sortSchema
   * @param colStats
   * @param ranges
   */
  public static void setMaxRangeIfNull(SortSpec[] sortSpecs, Schema sortSchema,
                                       List<ColumnStats> colStats, TupleRange[] ranges) {
    Map<Column, ColumnStats> statMap = Maps.newHashMap();
    for (ColumnStats stat : colStats) {
      statMap.put(stat.getColumn(), stat);
    }

    int i = 0;
    for (Column col : sortSchema.getRootColumns()) {
      ColumnStats columnStat = statMap.get(col);
      if (columnStat == null) {
        continue;
      }
      if (columnStat.hasNullValue()) {
        if (sortSpecs[i].isNullsFirst()) {
          Tuple rangeTuple = ranges[0].getStart();
          rangeTuple.put(i, NullDatum.get());
        } else {
          Tuple rangeTuple = ranges[ranges.length - 1].getEnd();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Set null into range: " + col.getQualifiedName() + ", previous tuple is " + rangeTuple);
          }
          rangeTuple.put(i, NullDatum.get());
          LOG.info("Set null into range: " + col.getQualifiedName() + ", current tuple is " + rangeTuple);
        }
      }
      i++;
    }
  }

  public static TupleRange columnStatToRange(SortSpec [] sortSpecs, Schema target, List<ColumnStats> colStats,
                                             boolean checkNull) {

    Map<Column, ColumnStats> statSet = Maps.newHashMap();
    for (ColumnStats stat : colStats) {
      statSet.put(stat.getColumn(), stat);
    }

    for (Column col : target.getRootColumns()) {
      Preconditions.checkState(statSet.containsKey(col),
          "ERROR: Invalid Column Stats (column stats: " + colStats + ", there exists not target " + col);
    }

    VTuple startTuple = new VTuple(target.size());
    VTuple endTuple = new VTuple(target.size());
    int i = 0;
    int sortSpecIndex = 0;

    // In outer join, empty table could be searched.
    // As a result, min value and max value would be null.
    // So, we should put NullDatum for this case.
    for (Column col : target.getRootColumns()) {
      if (sortSpecs[sortSpecIndex].isAscending()) {
        if (statSet.get(col).getMinValue() != null)
          startTuple.put(i, statSet.get(col).getMinValue());
        else
          startTuple.put(i, DatumFactory.createNullDatum());

        if (checkNull) {
          if (statSet.get(col).hasNullValue() || statSet.get(col).getMaxValue() == null)
            endTuple.put(i, DatumFactory.createNullDatum());
          else
            endTuple.put(i, statSet.get(col).getMaxValue());
        } else {
          if (statSet.get(col).getMaxValue() != null)
            endTuple.put(i, statSet.get(col).getMaxValue());
          else
            endTuple.put(i, DatumFactory.createNullDatum());
        }
      } else {
        if (checkNull) {
          if (statSet.get(col).hasNullValue() || statSet.get(col).getMaxValue() == null)
            startTuple.put(i, DatumFactory.createNullDatum());
          else
            startTuple.put(i, statSet.get(col).getMaxValue());
        } else {
          if (statSet.get(col).getMaxValue() != null)
            startTuple.put(i, statSet.get(col).getMaxValue());
          else
            startTuple.put(i, DatumFactory.createNullDatum());
        }
        if (statSet.get(col).getMinValue() != null)
          endTuple.put(i, statSet.get(col).getMinValue());
        else
          endTuple.put(i, DatumFactory.createNullDatum());
      }
      if (target.getRootColumns().size() == sortSpecs.length) {
        // Not composite column sort
        sortSpecIndex++;
      }
      i++;
    }
    return new TupleRange(sortSpecs, startTuple, endTuple);
  }
}
