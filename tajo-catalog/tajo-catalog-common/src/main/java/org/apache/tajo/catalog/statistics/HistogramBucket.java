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
import org.apache.tajo.json.GsonObject;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;

public class HistogramBucket implements ProtoObject<CatalogProtos.HistogramBucketProto>, Cloneable, GsonObject {
  
  @Expose private Double min = null; // required
  @Expose private Double max = null; // required
  @Expose private Long frequency = null; // required

  public HistogramBucket(double min, double max) {
    this.min = min;
    this.max = max;
    this.frequency = 0l;
  }
  
  public HistogramBucket(double min, double max, long frequency) {
    this.min = min;
    this.max = max;
    this.frequency = frequency;
  }

  public HistogramBucket(CatalogProtos.HistogramBucketProto proto) {
    if (proto.hasMin()) {
      this.min = proto.getMin();
    }
    if (proto.hasMax()) {
      this.max = proto.getMax();
    }
    if (proto.hasFrequency()) {
      this.frequency = proto.getFrequency();
    }
  }

  public Double getMin() {
    return this.min;
  }

  public Double getMax() {
    return this.max;
  }

  public Long getFrequency() {
    return this.frequency;
  }

  public void setMin(Double minVal) {
    this.min = minVal;
  }
  
  public void setMax(Double maxVal) {
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
      return getMin().equals(other.getMin())
          && getMax().equals(other.getMax())
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
      builder.setMin(this.min);
    }
    if (this.max != null) {
      builder.setMax(this.max);
    }
    if (this.frequency != null) {
      builder.setFrequency(this.frequency);
    }
    return builder.build();
  }
}
