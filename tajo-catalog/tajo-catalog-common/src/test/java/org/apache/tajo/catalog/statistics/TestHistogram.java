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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import org.apache.tajo.catalog.json.CatalogGsonHelper;
import org.apache.tajo.util.TUtil;
import org.junit.Test;

public class TestHistogram {

  private List<Double> generateRandomData(int numPoints, int maxBound) {
    List<Double> dataSet = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 0; i < numPoints; i++) {
      double p = rnd.nextDouble() * rnd.nextInt(maxBound);
      if(rnd.nextDouble() < 0.5) p = -p;
      dataSet.add(p);
    }
    return dataSet;
  }

  private List<Double> generateGaussianData(int numPoints, int scaleFactor, int numClusters) {
    List<Double> dataSet = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    int shiftStep = scaleFactor / (numClusters * 2);
    int clusterCount = 0;
    int i = -((int) Math.floor(numClusters / 2));
    
    while(clusterCount < numClusters) {
      int shiftAmount = i * shiftStep;
      for (int j = 0; j < numPoints; j++) {
	dataSet.add(rnd.nextGaussian() * scaleFactor + shiftAmount);
      }
      i++;
      clusterCount++;
    }
    return dataSet;
  }

  // samplingRatio must be in the range [0..1]
  private List<Double> getRandomSamples(List<Double> dataSet, double samplingRatio) {
    int dataSetSize = dataSet.size();
    List<Double> samples = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 0; i < dataSetSize; i++) {
      if (rnd.nextDouble() <= samplingRatio) {
	samples.add(dataSet.get(i));
      }
    }
    return samples;
  }

  private Double computeRealSelectivity(List<Double> points, Double from, Double to) {
    int numSatisfy = 0;
    for (Double p : points) {
      if (p >= from && p <= to) {
	numSatisfy++;
      }
    }
    return ((double) numSatisfy) / points.size();
  }

  private double computeListAverage(List<Double> values) {
    double sum = 0.0;
    for (Double v : values) {
      sum += v;
    }
    double avg = sum / values.size();
    return avg;
  }

  @Test
  public final void testHistogramAccuracy() {
    testHistogramAccuracy(0);
    testHistogramAccuracy(1);
    testHistogramAccuracy(3);
    testHistogramAccuracy(5);
  }

  private void testHistogramAccuracy(int dataType) {
    int valueBound = 100;
    int dataSize = 100000;
    double samplingRatio = 0.1;
    List<Double> dataSet;

    if (dataType == 0) {
      dataSet = generateRandomData(dataSize, valueBound);
      System.out.println("Random data\n");
    } else {
      dataSet = generateGaussianData(dataSize, valueBound, dataType);
      System.out.println("\nGaussian data (" + dataType + " clusters)\n");
    }
    List<Double> samples = getRandomSamples(dataSet, samplingRatio);

    System.out.println("+ Big test ranges");
    testHistogramAccuracy(0, dataSet, samples, valueBound, 20);
    testHistogramAccuracy(1, dataSet, samples, valueBound, 20);

    System.out.println("+ Small test ranges");
    testHistogramAccuracy(0, dataSet, samples, valueBound, 5);
    testHistogramAccuracy(1, dataSet, samples, valueBound, 5);
  }

  private double testHistogramAccuracy(int histogramType, List<Double> dataSet, List<Double> samples, int valueBound,
      int maxTestRange) {
    Histogram h;
    Double from, to, estimate, real, error;
    Random r = new Random(System.currentTimeMillis());
    long startTime, elapsedTime;
    List<Double> relativeErrors = TUtil.newList();

    if (histogramType == 0) {
      h = new EquiWidthHistogram();
      System.out.println("  EquiWidthHistogram");
    } else {
      h = new EquiDepthHistogram();
      System.out.println("  EquiDepthHistogram");
    }

    startTime = System.nanoTime();
    h.construct(samples);
    elapsedTime = (System.nanoTime() - startTime) / 1000000;
    assertTrue(h.getIsReady() == true);

    for (int i = 0; i < 1000; i++) {
      from = r.nextDouble() + r.nextInt(valueBound) * 1.2;
      if (r.nextDouble() < 0.5) from = -from;
      to = from + r.nextDouble() * maxTestRange;

      estimate = h.estimateSelectivity(from, to);
      real = computeRealSelectivity(dataSet, from, to);
      if (real.doubleValue() == 0) {
	if (estimate.doubleValue() == 0) {
	  error = 0.0;
	} else {
	  error = 1.0;
	}
      } else {
	error = Math.abs(real - estimate) / real;
      }
      relativeErrors.add(error);
    }
    double avgAccuracy = (1.0 - computeListAverage(relativeErrors)) * 100.0;
    System.out.format("    Construction time: %d ms; average accuracy: %.3f %%\n", elapsedTime, avgAccuracy);
    return avgAccuracy;
  }

  @Test
  public final void testEquiWidthCase1() {
    List<Double> samples = TUtil.newList(0.6, 1.1, 1.25, 1.7, 1.9, 5.0, 5.0, 8.0, 9.0, 9.7);
    EquiWidthHistogram h = new EquiWidthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 5);
    assertTrue(h.getBuckets().get(0).getMin() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax() == 1.9);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin() == 5);
    assertTrue(h.getBuckets().get(1).getMax() == 5);
  }

  @Test
  public final void testEquiWidthCase2() {
    List<Double> samples = TUtil.newList(-6.0, -5.1, -1.25, -1.17, 0.0, 3.2, 5.0, 8.0, 9.0, 9.7);
    EquiWidthHistogram h = new EquiWidthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax() == -5.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 3);
    assertTrue(h.getBuckets().get(1).getMin() == -1.25);
    assertTrue(h.getBuckets().get(1).getMax() == 0.0);
    assertTrue(h.getBuckets().get(2).getFrequency() == 1);
    assertTrue(h.getBuckets().get(2).getMin() == 3.2);
    assertTrue(h.getBuckets().get(2).getMax() == 3.2);
    assertTrue(h.getBuckets().get(4).getFrequency() == 3);
    assertTrue(h.getBuckets().get(4).getMin() == 8.0);
    assertTrue(h.getBuckets().get(4).getMax() == 9.7);
  }

  @Test
  public final void testEquiWidthCase3() {
    List<Double> samples = TUtil.newList(-6.0, -5.1, -1.25, -1.17, 0.0, -3.2, -5.0, -8.0, -9.0, -9.7);
    EquiWidthHistogram h = new EquiWidthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 3);
    assertTrue(h.getBuckets().get(0).getMin() == -9.7);
    assertTrue(h.getBuckets().get(0).getMax() == -8.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 3);
    assertTrue(h.getBuckets().get(4).getMin() == -1.25);
    assertTrue(h.getBuckets().get(4).getMax() == 0.0);
  }

  @Test
  public final void testEquiDepthCase1() {
    List<Double> samples = TUtil.newList(0.6, 1.1, 1.25, 1.7, 1.9, 5.0, 5.0, 8.0, 9.0, 9.7);
    EquiDepthHistogram h = new EquiDepthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax() == 1.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin() == 1.25);
    assertTrue(h.getBuckets().get(1).getMax() == 1.7);
    assertTrue(h.getBuckets().get(2).getFrequency() == 3);
    assertTrue(h.getBuckets().get(2).getMin() == 1.9);
    assertTrue(h.getBuckets().get(2).getMax() == 5.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 1);
    assertTrue(h.getBuckets().get(4).getMin() == 9.7);
    assertTrue(h.getBuckets().get(4).getMax() == 9.7);
  }

  @Test
  public final void testEquiDepthCase2() {
    List<Double> samples = TUtil.newList(0.6, 1.1, 1.25, 1.7, 1.9, 5.0, 5.0, 8.0, 9.0, 9.7, 9.8);
    EquiDepthHistogram h = new EquiDepthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 3);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 4);
    assertTrue(h.getBuckets().get(0).getMin() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax() == 1.7);
    assertTrue(h.getBuckets().get(1).getFrequency() == 4);
    assertTrue(h.getBuckets().get(1).getMin() == 1.9);
    assertTrue(h.getBuckets().get(1).getMax() == 8.0);
    assertTrue(h.getBuckets().get(2).getFrequency() == 3);
    assertTrue(h.getBuckets().get(2).getMin() == 9.0);
    assertTrue(h.getBuckets().get(2).getMax() == 9.8);
  }

  @Test
  public final void testEquiDepthCase3() {
    List<Double> samples = TUtil.newList(0.6, 1.1, 1.25, 1.7, 1.9, 5.0, 5.6, 8.0, 9.0, 9.7, 9.8);
    EquiDepthHistogram h = new EquiDepthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 2);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 6);
    assertTrue(h.getBuckets().get(0).getMin() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax() == 5.0);
    assertTrue(h.getBuckets().get(1).getFrequency() == 5);
    assertTrue(h.getBuckets().get(1).getMin() == 5.6);
    assertTrue(h.getBuckets().get(1).getMax() == 9.8);
  }

  @Test
  public final void testEquiDepthCase4() {
    List<Double> samples = TUtil.newList(-6.0, -5.1, -1.25, -1.17, 0.0, 3.2, 5.0, 8.0, 9.0, 9.7);
    EquiDepthHistogram h = new EquiDepthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() == 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax() == -5.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin() == -1.25);
    assertTrue(h.getBuckets().get(1).getMax() == -1.17);
    assertTrue(h.getBuckets().get(2).getFrequency() == 2);
    assertTrue(h.getBuckets().get(2).getMin() == 0.0);
    assertTrue(h.getBuckets().get(2).getMax() == 3.2);
    assertTrue(h.getBuckets().get(3).getFrequency() == 2);
    assertTrue(h.getBuckets().get(3).getMin() == 5.0);
    assertTrue(h.getBuckets().get(3).getMax() == 8.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 2);
    assertTrue(h.getBuckets().get(4).getMin() == 9.0);
    assertTrue(h.getBuckets().get(4).getMax() == 9.7);
  }

  @Test
  public final void testEquiDepthCase5() {
    List<Double> samples = TUtil.newList(-6.0, -5.1, -1.25, -1.17, 0.0, 3.2, 5.0, 8.0, 9.0, 9.7);
    EquiDepthHistogram h = new EquiDepthHistogram();
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 3);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() == 3);
    assertTrue(h.getBuckets().get(0).getFrequency() == 3);
    assertTrue(h.getBuckets().get(0).getMin() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax() == -1.25);
    assertTrue(h.getBuckets().get(1).getFrequency() == 3);
    assertTrue(h.getBuckets().get(1).getMin() == -1.17);
    assertTrue(h.getBuckets().get(1).getMax() == 3.2);
    assertTrue(h.getBuckets().get(2).getFrequency() == 4);
    assertTrue(h.getBuckets().get(2).getMin() == 5.0);
    assertTrue(h.getBuckets().get(2).getMax() == 9.7);
  }

  @Test
  public final void testEqualsObjectEquiWidth() {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiWidthHistogram();
    h1.construct(samples);

    Histogram h2 = new Histogram(h1.getProto());
    assertEquals(h1, h2);
  }

  @Test
  public final void testEqualsObjectEquiDepth() {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiDepthHistogram();
    h1.construct(samples);

    Histogram h2 = new Histogram(h1.getProto());
    assertEquals(h1, h2);
  }

  @Test
  public final void testJsonEquiWidth() throws CloneNotSupportedException {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiWidthHistogram();
    h1.construct(samples);

    String json = h1.toJson();
    Histogram h2 = CatalogGsonHelper.fromJson(json, Histogram.class);
    assertEquals(h1, h2);
  }

  @Test
  public final void testJsonEquiDepth() throws CloneNotSupportedException {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiDepthHistogram();
    h1.construct(samples);

    String json = h1.toJson();
    Histogram h2 = CatalogGsonHelper.fromJson(json, Histogram.class);
    assertEquals(h1, h2);
  }

  @Test
  public final void testCloneEquiWidth() throws CloneNotSupportedException {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiWidthHistogram();
    h1.construct(samples);

    Histogram h2 = h1.clone();
    assertEquals(h1, h2);
  }

  @Test
  public final void testCloneEquiDepth() throws CloneNotSupportedException {
    List<Double> samples = generateRandomData(1000, 100);
    Histogram h1 = new EquiDepthHistogram();
    h1.construct(samples);

    Histogram h2 = h1.clone();
    assertEquals(h1, h2);
  }
}
