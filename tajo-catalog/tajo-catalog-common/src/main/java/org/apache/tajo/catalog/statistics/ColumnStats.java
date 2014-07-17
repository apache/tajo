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
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

public class ColumnStats implements ProtoObject<CatalogProtos.ColumnStatsProto>, Cloneable, GsonObject {
  private CatalogProtos.ColumnStatsProto.Builder builder = CatalogProtos.ColumnStatsProto.newBuilder();

  @Expose private Column column = null; // required
  @Expose private Long numDistVals = null; // optional
  @Expose private Long numNulls = null; // optional
  @Expose private Datum minValue = null; // optional
  @Expose private Datum maxValue = null; // optional

  public ColumnStats(Column column) {
    this.column = column;
    numDistVals = 0l;
    numNulls = 0l;
  }

  public ColumnStats(CatalogProtos.ColumnStatsProto proto) {
    this.column = new Column(proto.getColumn());

    if (proto.hasNumDistVal()) {
      this.numDistVals = proto.getNumDistVal();
    }
    if (proto.hasNumNulls()) {
      this.numNulls = proto.getNumNulls();
    }
    if (proto.hasMinValue()) {
      this.minValue = DatumFactory.createFromBytes(getColumn().getDataType(), proto.getMinValue().toByteArray());
    }
    if (proto.hasMaxValue()) {
      this.maxValue = DatumFactory.createFromBytes(getColumn().getDataType(), proto.getMaxValue().toByteArray());
    }
  }

  public Column getColumn() {
    return this.column;
  }

  public Long getNumDistValues() {
    return this.numDistVals;
  }

  public void setNumDistVals(long numDistVals) {
    this.numDistVals = numDistVals;
  }

  public boolean minIsNotSet() {
    return minValue == null;
  }

  public Datum getMinValue() {
    return this.minValue;
  }

  public void setMinValue(Datum minValue) {
    this.minValue = minValue;
  }

  public boolean maxIsNotSet() {
    return maxValue == null;
  }

  public Datum getMaxValue() {
    return this.maxValue;
  }

  public void setMaxValue(Datum maxValue) {
    this.maxValue = maxValue;
  }

  public Long getNumNulls() {
    return this.numNulls;
  }

  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public boolean hasNullValue() {
    return numNulls > 0;
  }

  public boolean equals(Object obj) {
    if (obj instanceof ColumnStats) {
      ColumnStats other = (ColumnStats) obj;
      return getColumn().equals(other.getColumn())
          && getNumDistValues().equals(other.getNumDistValues())
          && getNumNulls().equals(other.getNumNulls())
          && TUtil.checkEquals(getMinValue(), other.getMinValue())
          && TUtil.checkEquals(getMaxValue(), other.getMaxValue());
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(getNumDistValues(), getNumNulls());
  }

  public Object clone() throws CloneNotSupportedException {
    ColumnStats stat = (ColumnStats) super.clone();
    stat.builder = CatalogProtos.ColumnStatsProto.newBuilder();
    stat.column = this.column;
    stat.numDistVals = numDistVals;
    stat.numNulls = numNulls;
    stat.minValue = minValue;
    stat.maxValue = maxValue;

    return stat;
  }

  public String toString() {
    return CatalogGsonHelper.getPrettyInstance().toJson(this, ColumnStats.class);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, ColumnStats.class);
  }


  @Override
  public CatalogProtos.ColumnStatsProto getProto() {
    if (builder == null) {
      builder = CatalogProtos.ColumnStatsProto.newBuilder();
    }
    if (this.column != null) {
      builder.setColumn(this.column.getProto());
    }
    if (this.numDistVals != null) {
      builder.setNumDistVal(this.numDistVals);
    }
    if (this.numNulls != null) {
      builder.setNumNulls(this.numNulls);
    }
    if (this.minValue != null) {
      builder.setMinValue(ByteString.copyFrom(this.minValue.asByteArray()));
    }
    if (this.maxValue != null) {
      builder.setMaxValue(ByteString.copyFrom(this.maxValue.asByteArray()));
    }

    return builder.build();
  }
}
