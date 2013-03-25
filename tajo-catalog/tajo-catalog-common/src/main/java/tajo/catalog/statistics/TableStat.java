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
package tajo.catalog.statistics;

import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import tajo.catalog.proto.CatalogProtos.ColumnStatProto;
import tajo.catalog.proto.CatalogProtos.TableStatProto;
import tajo.catalog.proto.CatalogProtos.TableStatProtoOrBuilder;
import tajo.common.ProtoObject;

import java.util.ArrayList;
import java.util.List;

public class TableStat implements ProtoObject<TableStatProto>, Cloneable {
  private TableStatProto proto = TableStatProto.getDefaultInstance();
  private TableStatProto.Builder builder = null;
  private boolean viaProto = false;

  @Expose private Long numRows = null;
  @Expose private Long numBytes = null;
  @Expose private Integer numBlocks = null;
  @Expose private Integer numPartitions = null;
  @Expose private Long avgRows = null;
  @Expose private List<ColumnStat> columnStats = null;

  public TableStat() {
    builder = TableStatProto.newBuilder();
    numRows = 0l;
    numBytes = 0l;
    numBlocks = 0;
    numPartitions = 0;
    avgRows = 0l;
  }

  public TableStat(TableStatProto proto) {
    this.proto = proto;
    this.viaProto = true;
  }

  public Long getNumRows() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if(numRows != null) {
      return this.numRows;
    }
    if(!p.hasNumRows()) {
      return 0l;
    }
    this.numRows = p.getNumRows();

    return this.numRows;
  }

  public void setNumRows(long numRows) {
    setModified();
    this.numRows = numRows;
  }

  public Integer getNumBlocks() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if(numBlocks != null) {
      return this.numBlocks;
    }
    if(!p.hasNumBlocks()) {
      return 0;
    }
    this.numBlocks = p.getNumBlocks();

    return this.numBlocks;
  }

  public void setNumBytes(long numBytes) {
    setModified();
    this.numBytes = numBytes;
  }

  public Long getNumBytes() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if (numBytes != null) {
      return this.numBytes;
    }
    if (!p.hasNumBytes()) {
      return 0l;
    }
    this.numBytes = p.getNumBytes();
    return this.numBytes;
  }

  public void setNumBlocks(int numBlocks) {
    setModified();
    this.numBlocks = numBlocks;
  }

  public Integer getNumPartitions() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if(numPartitions != null) {
      return this.numPartitions;
    }
    if(!p.hasNumPartitions()) {
      return 0;
    }
    this.numPartitions = p.getNumPartitions();

    return this.numPartitions;
  }

  public void setNumPartitions(int numPartitions) {
    setModified();
    this.numPartitions = numPartitions;
  }

  public Long getAvgRows() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if(avgRows != null) {
      return this.avgRows;
    }
    if(!p.hasAvgRows()) {
      return 0l;
    }
    this.avgRows = p.getAvgRows();

    return this.avgRows;
  }

  public void setAvgRows(long avgRows) {
    setModified();
    this.avgRows = avgRows;
  }

  public List<ColumnStat> getColumnStats() {
    initColumnStats();
    return this.columnStats;
  }

  public void setColumnStats(List<ColumnStat> columnStats) {
    setModified();
    this.columnStats = new ArrayList<ColumnStat>(columnStats);
  }

  public void addColumnStat(ColumnStat columnStat) {
    initColumnStats();
    this.columnStats.add(columnStat);
  }

  public boolean equals(Object obj) {
    if (obj instanceof TableStat) {
      TableStat other = (TableStat) obj;
      initFromProto();
      other.initFromProto();

      return this.numRows.equals(other.numRows)
          && this.numBytes.equals(other.numBytes)
          && this.numBlocks.equals(other.numBlocks)
          && this.numPartitions.equals(other.numPartitions)
          && this.avgRows.equals(other.avgRows)
          && columnStats.equals(other.columnStats);
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
    initFromProto();
    stat.numRows = numRows;
    stat.numBytes = numBytes;
    stat.numBlocks = numBlocks;
    stat.numPartitions = numPartitions;
    stat.columnStats = new ArrayList<ColumnStat>(this.columnStats);

    return stat;
  }

  public String toString() {
    Gson gson = new GsonBuilder().setPrettyPrinting().
        excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
  }

  private void initColumnStats() {
    if (this.columnStats != null) {
      return;
    }
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    this.columnStats = new ArrayList<ColumnStat>();
    for (ColumnStatProto colProto : p.getColStatList()) {
      columnStats.add(new ColumnStat(colProto));
    }
  }

  private void setModified() {
    if (viaProto && builder == null) {
      builder = TableStatProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public void initFromProto() {
    TableStatProtoOrBuilder p = viaProto ? proto : builder;
    if (this.numRows == null && p.hasNumRows()) {
      this.numRows = p.getNumRows();
    }
    if (this.numBytes == null && p.hasNumBytes()) {
      this.numBytes = p.getNumBytes();
    }
    if (this.numBlocks == null && p.hasNumBlocks()) {
      this.numBlocks = p.getNumBlocks();
    }
    if (this.numPartitions == null && p.hasNumPartitions()) {
      this.numPartitions = p.getNumPartitions();
    }
    if (this.avgRows == null && p.hasAvgRows()) {
      this.avgRows = p.getAvgRows();
    }

    initColumnStats();
    for (ColumnStat col : columnStats) {
      col.initFromProto();
    }
  }

  @Override
  public TableStatProto getProto() {
    if (!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }

    return proto;
  }

  private void mergeLocalToBuilder() {
    if (builder == null) {
      builder = TableStatProto.newBuilder(proto);
    }
    if (this.numRows != null) {
      builder.setNumRows(this.numRows);
    }
    if (this.numBytes != null) {
      builder.setNumBytes(this.numBytes);
    }
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
  }
}