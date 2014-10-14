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
import java.util.Collections;
import java.util.List;

import org.apache.tajo.catalog.proto.CatalogProtos.HistogramProto;
import org.apache.tajo.util.TUtil;

public class EquiDepthHistogram extends Histogram {

  public EquiDepthHistogram() {
    super();
  }

  public EquiDepthHistogram(HistogramProto proto) {
    super(proto);
  }

  @Override
  public boolean construct(List<Double> samples) {
    int numBuckets = samples.size() > DEFAULT_MAX_BUCKETS ? DEFAULT_MAX_BUCKETS : samples.size();
    return construct(samples, numBuckets);
  }

  /**
   * An EquiDepth histogram is constructed by dividing the data points into buckets of equal frequencies (as equal as
   * possible).
   * 
   * Note that, this function, which allows the specification of the (maximum) number of buckets, should be called
   * directly only in the unit tests. In non-test cases, the construct(samples) version above should be used.
   */
  public boolean construct(List<Double> srcSamples, int numBuckets) {
    isReady = false;
    buckets = TUtil.newList();

    ArrayList<Double> samples = new ArrayList<Double>(srcSamples); // sorted samples
    Collections.sort(samples);

    int averageFrequency = Math.round((float) samples.size() / numBuckets);
    int bFrequency;
    int bMinIndex = 0, bMaxIndex;
    Double bMin, bMax;

    for (int i = 0; i < numBuckets && bMinIndex < samples.size(); i++) {
      bMin = samples.get(bMinIndex);
      bFrequency = averageFrequency;
      bMaxIndex = bMinIndex + averageFrequency - 1;

      if (bMaxIndex > samples.size() - 1) {
	bMaxIndex = samples.size() - 1;
	bFrequency = bMaxIndex - bMinIndex + 1;
      }

      while (bMaxIndex + 1 < samples.size() && samples.get(bMaxIndex).equals(samples.get(bMaxIndex + 1))) {
	bMaxIndex++;
	bFrequency++;
      }
      bMax = samples.get(bMaxIndex);
      bMinIndex = bMaxIndex + 1;
      buckets.add(new HistogramBucket(bMin, bMax, bFrequency));
    }

    if (bMinIndex < samples.size()) {
      int lastBucket = buckets.size() - 1;
      int additionalFrequency = samples.size() - bMinIndex;
      buckets.get(lastBucket).setFrequency(buckets.get(lastBucket).getFrequency() + additionalFrequency);
      buckets.get(lastBucket).setMax(samples.get(samples.size() - 1));
    }

    samples.clear();
    isReady = true;
    lastAnalyzed = System.currentTimeMillis();

    return true;
  }
}
