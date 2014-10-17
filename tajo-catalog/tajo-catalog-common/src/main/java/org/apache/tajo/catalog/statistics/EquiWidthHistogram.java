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

import org.apache.tajo.catalog.proto.CatalogProtos.HistogramProto;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.TUtil;

import com.google.common.annotations.VisibleForTesting;

public class EquiWidthHistogram extends Histogram {

  public EquiWidthHistogram(Type dataType) {
    super(dataType);
  }

  public EquiWidthHistogram(HistogramProto proto) {
    super(proto);
  }

  @Override
  public boolean construct(List<Datum> samples) {
    int numBuckets = samples.size() > DEFAULT_MAX_BUCKETS ? DEFAULT_MAX_BUCKETS : samples.size();
    return construct(samples, numBuckets);
  }
  
  /**
   * Originally, an EquiWidth histogram is constructed by dividing the entire value range of the data points into
   * buckets of equal sizes. Here, we construct it similarly, then improve it by removing empty buckets, to save disk
   * space and processing time, and by compacting the buckets' boundaries, to increase selectivity estimation accuracy.
   * 
   * Note that, this function, which allows the specification of the (maximum) number of buckets, should be called
   * directly only in the unit tests. In non-test cases, the construct(samples) version above should be used.
   */
  @VisibleForTesting
  public boolean construct(List<Datum> samples, int numBuckets) {
    isReady = false;
    buckets = TUtil.newList();
    Datum globalMin = Datum.getRangeMax(this.dataType);
    Datum globalMax = Datum.getRangeMin(this.dataType);
    List<Datum> bMinValues = new ArrayList<Datum>(numBuckets);
    List<Datum> bMaxValues = new ArrayList<Datum>(numBuckets);
    List<Long> bFrequencies = new ArrayList<Long>(numBuckets);

    for (Datum p : samples) {
      if (p.lessThan(globalMin).asBool() == true) globalMin = p;
      if (p.greaterThan(globalMax).asBool() == true) globalMax = p;
    }
    double bWidth = (globalMax.asFloat8() - globalMin.asFloat8()) / numBuckets;

    for (int i = 0; i < numBuckets; i++) {
      bMinValues.add(Datum.getRangeMax(this.dataType));
      bMaxValues.add(Datum.getRangeMin(this.dataType));
      bFrequencies.add(0l);
    }

    for (Datum p : samples) {
      int bIndex = (int) Math.round(Math.floor((p.asFloat8() - globalMin.asFloat8()) / bWidth));
      if (bIndex > numBuckets - 1) bIndex = numBuckets - 1;
      bFrequencies.set(bIndex, bFrequencies.get(bIndex) + 1);

      if (p.lessThan(bMinValues.get(bIndex)).asBool() == true) bMinValues.set(bIndex, p);
      if (p.greaterThan(bMaxValues.get(bIndex)).asBool() == true) bMaxValues.set(bIndex, p);
    }

    for (int i = 0; i < numBuckets; i++) {
      if (bFrequencies.get(i).longValue() > 0) {
	buckets.add(new HistogramBucket(bMinValues.get(i), bMaxValues.get(i), bFrequencies.get(i)));
      }
    }

    isReady = true;
    lastAnalyzed = System.currentTimeMillis();

    return true;
  }
}
