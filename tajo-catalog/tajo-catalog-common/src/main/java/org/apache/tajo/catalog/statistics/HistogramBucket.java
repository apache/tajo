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

import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.HistogramBucketProto;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;

public class HistogramBucket implements ProtoObject<CatalogProtos.HistogramBucketProto>, Cloneable, GsonObject {
  
  @Expose private Datum min = null; // required
  @Expose private Datum max = null; // required
  @Expose private Long frequency = null; // required 

  public HistogramBucket(Datum min, Datum max) {
    this.min = min;
    this.max = max;
    this.frequency = 0l;
  }
  
  public HistogramBucket(Datum min, Datum max, long frequency) {
    this(min, max);
    this.frequency = frequency;
  }

  public HistogramBucket(CatalogProtos.HistogramBucketProto proto, Type dataType) {
    DataType.Builder typeBuilder = DataType.newBuilder();
    typeBuilder.setType(dataType);
    
    if (proto.hasMin()) {
      this.min = DatumFactory.createFromBytes(typeBuilder.build(), proto.getMin().toByteArray());
    }
    if (proto.hasMax()) {
      this.max = DatumFactory.createFromBytes(typeBuilder.build(), proto.getMax().toByteArray());
    }
    if (proto.hasFrequency()) {
      this.frequency = proto.getFrequency();
    }
  }

  public Datum getMin() {
    return this.min;
  }

  public Datum getMax() {
    return this.max;
  }

  public Long getFrequency() {
    return this.frequency;
  }

  public void setMin(Datum minVal) {
    this.min = minVal;
  }
  
  public void setMax(Datum maxVal) {
    this.max = maxVal;
  }
  
  public void setFrequency(Long freqVal) {
    if(freqVal < 0) {
      this.frequency = 0l;
    } else {
      this.frequency = freqVal;
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof HistogramBucket) {
      HistogramBucket other = (HistogramBucket) obj;
      return TUtil.checkEquals(getMin(), other.getMin())
          && TUtil.checkEquals(getMax(), other.getMax())
          && getFrequency().equals(other.getFrequency());
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(getMin(), getMax(), getFrequency());
  }

  public Object clone() throws CloneNotSupportedException {
    HistogramBucket buk = (HistogramBucket) super.clone();
    buk.min = this.min;
    buk.max = this.max;
    buk.frequency = this.frequency;

    return buk;
  }

  public String toString() {
    return CatalogGsonHelper.getPrettyInstance().toJson(this, HistogramBucket.class);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, HistogramBucket.class);
  }


  @Override
  public HistogramBucketProto getProto() {
    CatalogProtos.HistogramBucketProto.Builder builder = CatalogProtos.HistogramBucketProto.newBuilder();
    if (this.min != null) {
      builder.setMin(ByteString.copyFrom(this.min.asByteArray()));
    }
    if (this.max != null) {
      builder.setMax(ByteString.copyFrom(this.max.asByteArray()));
    }
    if (this.frequency != null) {
      builder.setFrequency(this.frequency);
    }
    return builder.build();
  }
}
