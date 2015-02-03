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

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.json.GsonObject;
import org.apache.tajo.util.TUtil;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;

public class Histogram implements ProtoObject<CatalogProtos.HistogramProto>, Cloneable, GsonObject {
  
  @Expose
  protected Long lastAnalyzed = null; // optional, store the last time point that this histogram is constructed,
				      // will be used together with the "last-updated time" of the column data
				      // to decide whether this histogram needs to be reconstructed or not
  @Expose protected Type dataType = null; // required
  @Expose protected List<HistogramBucket> buckets = null; // repeated
  @Expose protected boolean isReady; // whether this histogram is ready to be used for selectivity estimation
  protected int DEFAULT_MAX_BUCKETS = 100; // same as PostgreSQL

  public Histogram(Type dataType) {
    if (dataType != Type.INT2 && dataType != Type.INT4 && dataType != Type.INT8 && dataType != Type.FLOAT4
	&& dataType != Type.FLOAT8) {
      throw new UnsupportedException();
    }
    this.dataType = dataType;
    this.buckets = TUtil.newList();
    this.isReady = false;
  }
  
  public Histogram(CatalogProtos.HistogramProto proto) {
    if (proto.hasDataType()) {
      this.dataType = proto.getDataType();
    }
    if (proto.hasLastAnalyzed()) {
      this.lastAnalyzed = proto.getLastAnalyzed();
    }
    buckets = TUtil.newList();
    for (CatalogProtos.HistogramBucketProto bucketProto : proto.getBucketsList()) {
      this.buckets.add(new HistogramBucket(bucketProto, dataType));
    }
    isReady = true;
  }
  
  public Long getLastAnalyzed() {
    return this.lastAnalyzed;
  }
  
  public void setLastAnalyzed(Long lastAnalyzed) {
    this.lastAnalyzed = lastAnalyzed;
  }
  
  public Type getDataType() {
    return this.dataType;
  }
  
  public void setDataType(Type dataType) {
    this.dataType = dataType;
  }
  
  public List<HistogramBucket> getBuckets() {
    return this.buckets;
  }

  public void setBuckets(List<HistogramBucket> buckets) {
    this.buckets = new ArrayList<HistogramBucket>(buckets);
  }

  public void addBucket(HistogramBucket bucket) {
    this.buckets.add(bucket);
  }

  public int getBucketsCount() {
    return this.buckets.size();
  }

  public boolean getIsReady() {
    return this.isReady;
  }
  
  public void setIsReady(boolean isReady) {
    this.isReady = isReady;
  }
  
  public boolean equals(Object obj) {
    if (obj instanceof Histogram) {
      Histogram other = (Histogram) obj;
      return getLastAnalyzed().equals(other.getLastAnalyzed())
	  && TUtil.checkEquals(getDataType(), other.getDataType())
          && TUtil.checkEquals(this.buckets, other.buckets);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hashCode(this.lastAnalyzed, this.buckets);
  }

  public Histogram clone() throws CloneNotSupportedException {
    Histogram hist = (Histogram) super.clone();
    hist.lastAnalyzed = this.lastAnalyzed;
    hist.dataType = this.dataType;
    hist.buckets = new ArrayList<HistogramBucket>(this.buckets);
    hist.isReady = this.isReady;
    return hist;
  }

  public String toString() {
    return CatalogGsonHelper.getPrettyInstance().toJson(this, Histogram.class);
  }

  @Override
  public String toJson() {
    return CatalogGsonHelper.toJson(this, Histogram.class);
  }


  @Override
  public CatalogProtos.HistogramProto getProto() {
    CatalogProtos.HistogramProto.Builder builder = CatalogProtos.HistogramProto.newBuilder();
    if (this.lastAnalyzed != null) {
      builder.setLastAnalyzed(this.lastAnalyzed);
    }
    if (this.dataType != null) {
      builder.setDataType(this.dataType);
    }
    if (this.buckets != null) {
      for (HistogramBucket bucket : buckets) {
        builder.addBuckets(bucket.getProto());
      }
    }
    return builder.build();
  }

  /**
   * Construct a histogram. Compute the number of buckets and the min, max, frequency values for each of them. This
   * method must be overridden by specific sub-classes. The number of buckets should be less than or equal to the
   * sample size.
   * 
   * @param samples
   *          Sample data points to construct the histogram. This collection should fit in the memory and Null values
   *          should never appear in it.
   * @return Return true if the computation is done without any problem. Otherwise, return false
   */
  public boolean construct(List<Datum> samples) {
    // When overridden in sub-classes, remember to update lastAnalyzed and isReady before returning
    return false;
  }

  /**
   * Estimate the selectivity. "from" must be less than or equal to "to". For example, in the query
   * "SELECT * from Employee WHERE age >= 20 and age <= 30", "from" is 20 and "to" is 30. If "from" is not specified in
   * the predicate, the minimum possible value should be used. Similarly, if "to" is not specified in the predicate, the
   * maximum possible value should be used. "from" and "to" are inclusive, which means that an epsilon value should be
   * added to "from", or subtracted from "to", if the comparison in the predicate is exclusive. For example, if the
   * selection condition in the above query is "age > 20", "from" should be 20 + E where E is any number that satisfies
   * "0 < E < 1".
   * 
   * @param from
   *          The inclusive lower bound
   * @param to
   *          The inclusive upper bound
   * @return The selectivity in range [0..1]. If the histogram is not ready (i.e., being constructed), return -1
   */
  public double estimateSelectivity(Datum from, Datum to) {
    if(from.greaterThan(to).asBool() == true) return 0;
    if(!isReady) return -1;
    Double freq = estimateFrequency(from, to);
    Double totalFreq = 0.0;
    for(HistogramBucket bucket : buckets) {
      totalFreq += bucket.getFrequency();
    }
    double selectivity = freq / totalFreq;
    return selectivity;
  }

  /**
   * Based on the histogram's buckets, estimate the number of rows whose values (of the corresponding column) are
   * between "from" and "to".
   * 
   * @param from
   * @param to
   * @return 
   */
  private Double estimateFrequency(Datum from, Datum to) {
    Double estimate = 0.0;
    for(HistogramBucket bucket : buckets) {
      estimate += estimateFrequency(bucket, from, to);
    }
    return estimate;
  }
  
  private Double estimateFrequency(HistogramBucket bucket, Datum from, Datum to) {
    Datum min = bucket.getMin();
    Datum max = bucket.getMax();
    Double width = max.asFloat8() - min.asFloat8();
    Double overlap = 0.0;
    
    if (min.lessThan(max).asBool() == true) {
      if (from.lessThan(min).asBool() == true) {
	if (to.lessThan(min).asBool() == true) {
	  overlap = 0.0;
	} else if (to.greaterThanEqual(min).asBool() == true && to.lessThanEqual(max).asBool() == true) {
	  overlap = (to.asFloat8() - min.asFloat8()) / width;
	} else {
	  overlap = 1.0;
	}
      } else if (from.greaterThanEqual(min).asBool() == true && from.lessThanEqual(max).asBool() == true) {
	if (to.lessThanEqual(max).asBool() == true) {
	  overlap = (to.asFloat8() - from.asFloat8()) / width;
	} else {
	  overlap = (max.asFloat8() - from.asFloat8()) / width;
	}
      } else {
	overlap = 0.0;
      }
    } else { // min == max
      if(min.greaterThanEqual(from).asBool() == true && min.lessThanEqual(to).asBool() == true) {
	overlap = 1.0;
      } else {
	overlap = 0.0;
      }
    }
    
    return overlap * bucket.getFrequency();
  }
}
