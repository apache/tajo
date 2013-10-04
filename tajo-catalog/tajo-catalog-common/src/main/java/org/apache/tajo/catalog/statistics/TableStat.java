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

/**
 *
 */
package org.apache.tajo.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnStatProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

public class TableStat implements ProtoObject<TableStatProto>, Cloneable, GsonObject {
  private TableStatProto.Builder builder = TableStatProto.newBuilder();

  @Expose private Long numRows = null; // required
  @Expose private Long numBytes = null; // required
  @Expose private Integer numBlocks = null; // optional
  @Expose private Integer numPartitions = null; // optional
  @Expose private Long avgRows = null; // optional
  @Expose private List<ColumnStat> columnStats = null; // repeated

  public TableStat() {
    numRows = 0l;
    numBytes = 0l;
    numBlocks = 0;
    numPartitions = 0;
    avgRows = 0l;
    columnStats = TUtil.newList();
  }

  public TableStat(TableStatProto proto) {
    this.numRows = proto.getNumRows();
    this.numBytes = proto.getNumBytes();

    if (proto.hasNumBlocks()) {
      this.numBlocks = proto.getNumBlocks();
    }
    if (proto.hasNumPartitions()) {
      this.numPartitions = proto.getNumPartitions();
    }
    if (proto.hasAvgRows()) {
      this.avgRows = proto.getAvgRows();
    }

    this.columnStats = TUtil.newList();
    for (ColumnStatProto colProto : proto.getColStatList()) {
      if (colProto.getColumn().getDataType().getType() == TajoDataTypes.Type.PROTOBUF) {
        continue;
      }
      columnStats.add(new ColumnStat(colProto));
    }
  }

  public Long getNumRows() {
    return this.numRows;
  }

  public void setNumRows(long numRows) {
    this.numRows = numRows;
  }

  public Integer getNumBlocks() {
    return this.numBlocks;
  }

  public void setNumBytes(long numBytes) {
    this.numBytes = numBytes;
  }

  public Long getNumBytes() {
    return this.numBytes;
  }

  public void setNumBlocks(int numBlocks) {
    this.numBlocks = numBlocks;
  }

  public Integer getNumPartitions() {
    return this.numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public Long getAvgRows() {
    return this.avgRows;
  }

  public void setAvgRows(long avgRows) {
    this.avgRows = avgRows;
  }

  public List<ColumnStat> getColumnStats() {
    return this.columnStats;
  }

  public void setColumnStats(List<ColumnStat> columnStats) {
    this.columnStats = new ArrayList<ColumnStat>(columnStats);
  }

  public void addColumnStat(ColumnStat columnStat) {
    this.columnStats.add(columnStat);
  }

  public boolean equals(Object obj) {
    if (obj instanceof TableStat) {
      TableStat other = (TableStat) obj;

      return this.numRows.equals(other.numRows)
          && this.numBytes.equals(other.numBytes)
          && TUtil.checkEquals(this.numBlocks, other.numBlocks)
          && TUtil.checkEquals(this.numPartitions, other.numPartitions)
          && TUtil.checkEquals(this.avgRows, other.avgRows)
          && TUtil.checkEquals(this.columnStats, other.columnStats);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(numRows, numBytes,
        numBlocks, numPartitions, columnStats);
  }

  public Object clone() throws CloneNotSupportedException {
    TableStat stat = (TableStat) super.clone();
    stat.builder = TableStatProto.newBuilder();
    stat.numRows = numRows;
    stat.numBytes = numBytes;
    stat.numBlocks = numBlocks;
    stat.numPartitions = numPartitions;
    stat.columnStats = new ArrayList<ColumnStat>(this.columnStats);

    return stat;
  }

  public String toString() {
    Gson gson = CatalogGsonHelper.getPrettyInstance();
    return gson.toJson(this);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, TableStat.class);
  }

  @Override
  public TableStatProto getProto() {
    if (builder == null) {
      builder = TableStatProto.newBuilder();
    } else {
      builder.clear();
    }

    builder.setNumRows(this.numRows);
    builder.setNumBytes(this.numBytes);

    if (this.numBlocks != null) {
      builder.setNumBlocks(this.numBlocks);
    }
    if (this.numPartitions != null) {
      builder.setNumPartitions(this.numPartitions);
    }
    if (this.avgRows != null) {
      builder.setAvgRows(this.avgRows);
    }
    if (this.columnStats != null) {
      for (ColumnStat colStat : columnStats) {
        builder.addColStat(colStat.getProto());
      }
    }
    return builder.build();
  }
}