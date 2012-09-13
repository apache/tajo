/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.util;

import tajo.catalog.statistics.Stat;
import tajo.catalog.statistics.StatSet;
import tajo.catalog.statistics.TableStat;
import tajo.common.exception.NotImplementedException;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.engine.planner.global.QueryUnit;

public class TQueryUtil {

  public static TableStat mergeStatSet(TableStat tableStat, StatSet statSet) {
    for (Stat stat : statSet.getAllStats()) {
      switch (stat.getType()) {
      case COLUMN_NUM_NULLS:
        // TODO
        throw new NotImplementedException();
      case TABLE_AVG_ROWS:
        if (tableStat.getAvgRows() == null) {
          tableStat.setAvgRows(stat.getValue());
        } else {
          tableStat.setAvgRows(tableStat.getAvgRows()+stat.getValue());
        }
        break;
      case TABLE_NUM_BLOCKS:
        if (tableStat.getNumBlocks() == null) {
          tableStat.setNumBlocks((int)stat.getValue());
        } else {
          tableStat.setNumBlocks(tableStat.getNumBlocks()+
              (int)stat.getValue());
        }
        break;
      case TABLE_NUM_BYTES:
        if (tableStat.getNumBytes() == null) {
          tableStat.setNumBytes(stat.getValue());
        } else {
          tableStat.setNumBytes(tableStat.getNumBytes()+stat.getValue());
        }
        break;
      case TABLE_NUM_PARTITIONS:
        if (tableStat.getNumPartitions() == null) {
          tableStat.setNumPartitions((int)stat.getValue());
        } else {
          tableStat.setNumPartitions(tableStat.getNumPartitions()+
              (int)stat.getValue());
        }
        break;
      case TABLE_NUM_ROWS:
        if (tableStat.getNumRows() == null) {
          tableStat.setNumRows(stat.getValue());
        } else {
          tableStat.setNumRows(tableStat.getNumRows()+stat.getValue());
        }
        break;
      }
    }
    
    return tableStat;
  }
  
  public static TaskStatusProto getInProgressStatusProto(QueryUnit unit) {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setId(unit.getLastAttempt().getId().getProto());
    builder.setStatus(unit.getStatus());
    builder.setProgress(unit.getProgress());
    builder.addAllPartitions(unit.getPartitions());
    builder.setResultStats(unit.getStats().getProto());
    return builder.build();
  }
}
