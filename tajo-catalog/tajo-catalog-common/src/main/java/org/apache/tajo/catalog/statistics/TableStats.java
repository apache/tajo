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
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos.TableStatsProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

public class TableStats implements ProtoObject<TableStatsProto>, Cloneable, GsonObject {
  private TableStatsProto.Builder builder = TableStatsProto.newBuilder();

  @Expose private Long numRows = null; // required
  @Expose private Long numBytes = null; // required
  @Expose private Integer numBlocks = null; // optional
  @Expose private Integer numShuffleOutputs = null; // optional
  @Expose private Long avgRows = null; // optional
  @Expose private Long readBytes = null; //optional
  @Expose private List<ColumnStats> columnStatses = null; // repeated

  public TableStats() {
    reset();
  }

  public void reset() {
    numRows = 0l;
    numBytes = 0l;
    numBlocks = 0;
    numShuffleOutputs = 0;
    avgRows = 0l;
    readBytes = 0l;
    columnStatses = TUtil.newList();
  }

  public TableStats(CatalogProtos.TableStatsProto proto) {
    this.numRows = proto.getNumRows();
    this.numBytes = proto.getNumBytes();

    if (proto.hasNumBlocks()) {
      this.numBlocks = proto.getNumBlocks();
    } else {
      this.numBlocks = 0;
    }
    if (proto.hasNumShuffleOutputs()) {
      this.numShuffleOutputs = proto.getNumShuffleOutputs();
    } else {
      this.numShuffleOutputs = 0;
    }
    if (proto.hasAvgRows()) {
      this.avgRows = proto.getAvgRows();
    } else {
      this.avgRows = 0l;
    }
    if (proto.hasReadBytes()) {
      this.readBytes = proto.getReadBytes();
    } else {
      this.readBytes = 0l;
    }

    this.columnStatses = TUtil.newList();
    for (CatalogProtos.ColumnStatsProto colProto : proto.getColStatList()) {
      if (colProto.getColumn().getDataType().getType() == TajoDataTypes.Type.PROTOBUF) {
        continue;
      }
      columnStatses.add(new ColumnStats(colProto));
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

  public Integer getNumShuffleOutputs() {
    return this.numShuffleOutputs;
  }

  public void setNumShuffleOutputs(int numOutputs) {
    this.numShuffleOutputs = numOutputs;
  }

  public Long getAvgRows() {
    return this.avgRows;
  }

  public void setAvgRows(long avgRows) {
    this.avgRows = avgRows;
  }

  public Long getReadBytes() {
    return readBytes;
  }

  public void setReadBytes(long readBytes) {
    this.readBytes = readBytes;
  }

  public List<ColumnStats> getColumnStats() {
    return this.columnStatses;
  }

  public void setColumnStats(List<ColumnStats> columnStatses) {
    this.columnStatses = new ArrayList<ColumnStats>(columnStatses);
  }

  public void addColumnStat(ColumnStats columnStats) {
    this.columnStatses.add(columnStats);
  }

  public boolean equals(Object obj) {
    if (obj instanceof TableStats) {
      TableStats other = (TableStats) obj;

      boolean eq = this.numRows.equals(other.numRows);
      eq = eq && this.numBytes.equals(other.numBytes);
      eq = eq && TUtil.checkEquals(this.numBlocks, other.numBlocks);
      eq = eq && TUtil.checkEquals(this.numShuffleOutputs, other.numShuffleOutputs);
      eq = eq && TUtil.checkEquals(this.avgRows, other.avgRows);
      eq = eq && TUtil.checkEquals(this.readBytes, other.readBytes);
      eq = eq && TUtil.checkEquals(this.columnStatses, other.columnStatses);
      return eq;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(numRows, numBytes,
        numBlocks, numShuffleOutputs, columnStatses);
  }

  public Object clone() throws CloneNotSupportedException {
    TableStats stat = (TableStats) super.clone();
    stat.builder = CatalogProtos.TableStatsProto.newBuilder();
    stat.numRows = numRows != null ? numRows : null;
    stat.numBytes = numBytes != null ? numBytes : null;
    stat.numBlocks = numBlocks != null ? numBlocks : null;
    stat.numShuffleOutputs = numShuffleOutputs != null ? numShuffleOutputs : null;
    stat.avgRows = avgRows != null ? avgRows : null;
    stat.readBytes = readBytes != null ? readBytes : null;

    stat.columnStatses = new ArrayList<ColumnStats>(this.columnStatses);

    return stat;
  }

  public void merge(TableStats stat) {
    if(stat == null) {
      return;
    }

    if (stat.numRows != null) {
      numRows += stat.numRows;
    }
    if (stat.numBytes != null) {
      numBytes += stat.numBytes;
    }
    if (stat.numBlocks != null) {
      numBlocks += stat.numBlocks;
    }
    if (stat.numShuffleOutputs != null) {
      numShuffleOutputs += stat.numShuffleOutputs;
    }
    if (stat.avgRows != null) {
      avgRows += stat.avgRows;
    }
    if (stat.readBytes != null) {
      readBytes += stat.readBytes;
    }
  }

  public void setValues(TableStats stat) {
    if(stat == null) {
      return;
    }

    numRows = stat.numRows != null ? stat.numRows : 0;
    numBytes = stat.numBytes != null ? stat.numBytes : 0;
    numBlocks = stat.numBlocks != null ? stat.numBlocks : 0;
    numShuffleOutputs = stat.numShuffleOutputs != null ? stat.numShuffleOutputs : 0;
    avgRows = stat.avgRows != null ? stat.avgRows : 0;
    readBytes = stat.readBytes != null ? stat.readBytes : 0;
  }

  public String toString() {
    Gson gson = CatalogGsonHelper.getPrettyInstance();
    return gson.toJson(this);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, TableStats.class);
  }

  @Override
  public TableStatsProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.TableStatsProto.newBuilder();
    } else {
      builder.clear();
    }

    builder.setNumRows(this.numRows);
    builder.setNumBytes(this.numBytes);

    if (this.numBlocks != null) {
      builder.setNumBlocks(this.numBlocks);
    }
    if (this.numShuffleOutputs != null) {
      builder.setNumShuffleOutputs(this.numShuffleOutputs);
    }
    if (this.avgRows != null) {
      builder.setAvgRows(this.avgRows);
    }
    if (this.readBytes != null) {
      builder.setReadBytes(this.readBytes);
    }
    if (this.columnStatses != null) {
      for (ColumnStats colStat : columnStatses) {
        builder.addColStat(colStat.getProto());
      }
    }
    return builder.build();
  }
}