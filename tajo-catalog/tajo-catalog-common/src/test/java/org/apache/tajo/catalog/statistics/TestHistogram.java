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
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.util.TUtil;
import org.junit.Test;

public class TestHistogram {
  private static int maxBound = 100;

  private List<Datum> generateRandomData(Type dataType, int numPoints) {
    List<Datum> dataSet = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 0; i < numPoints; i++) {

      switch (dataType) {
      case INT2:
	Integer i2 = rnd.nextInt(Short.MAX_VALUE);
	if (rnd.nextDouble() < 0.5) {
	  i2 = -i2;
	}
	dataSet.add(DatumFactory.createInt2(i2.shortValue()));
	break;

      case INT4:
	Integer i4 = rnd.nextInt();
	if (rnd.nextDouble() < 0.5) {
	  i4 = -i4;
	}
	dataSet.add(DatumFactory.createInt4(i4.intValue()));
	break;

      case INT8:
	Long i8 = rnd.nextLong();
	if (rnd.nextDouble() < 0.5) {
	  i8 = -i8;
	}
	dataSet.add(DatumFactory.createInt8(i8.longValue()));
	break;

      case FLOAT4:
	double f4 = rnd.nextFloat() * rnd.nextInt(maxBound);
	if (rnd.nextDouble() < 0.5) {
	  f4 = -f4;
	}
	dataSet.add(DatumFactory.createFloat8(f4));
	break;

      case FLOAT8:
	double f8 = rnd.nextDouble() * rnd.nextInt(maxBound);
	if (rnd.nextDouble() < 0.5) {
	  f8 = -f8;
	}
	dataSet.add(DatumFactory.createFloat8(f8));
	break;

      default:
	break;
      }
    }
    return dataSet;
  }

  private List<Datum> generateGaussianData(int numPoints, int scaleFactor, int numClusters) {
    List<Datum> dataSet = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    int shiftStep = scaleFactor / (numClusters * 2);
    int clusterCount = 0;
    int i = -((int) Math.floor(numClusters / 2));
    
    while(clusterCount < numClusters) {
      int shiftAmount = i * shiftStep;
      for (int j = 0; j < numPoints; j++) {
	dataSet.add(DatumFactory.createFloat8(rnd.nextGaussian() * scaleFactor + shiftAmount));
      }
      i++;
      clusterCount++;
    }
    return dataSet;
  }

  // samplingRatio must be in the range [0..1]
  private List<Datum> getRandomSamples(List<Datum> dataSet, double samplingRatio) {
    int dataSetSize = dataSet.size();
    List<Datum> samples = TUtil.newList();
    Random rnd = new Random(System.currentTimeMillis());
    for (int i = 0; i < dataSetSize; i++) {
      if (rnd.nextDouble() <= samplingRatio) {
	samples.add(dataSet.get(i));
      }
    }
    return samples;
  }

  private Double computeRealSelectivity(List<Datum> points, Datum from, Datum to) {
    int numSatisfy = 0;
    for (Datum p : points) {
      if (p.greaterThanEqual(from).asBool() == true && p.lessThanEqual(to).asBool() == true) {
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
    int dataSize = 100000;
    double samplingRatio = 0.1;
    List<Datum> dataSet;

    if (dataType == 0) {
      dataSet = generateRandomData(Type.FLOAT8, dataSize);
      System.out.println("Random data\n");
    } else {
      dataSet = generateGaussianData(dataSize, maxBound, dataType);
      System.out.println("\nGaussian data (" + dataType + " clusters)\n");
    }
    List<Datum> samples = getRandomSamples(dataSet, samplingRatio);

    System.out.println("+ Big test ranges");
    testHistogramAccuracy(0, dataSet, samples, 20);
    testHistogramAccuracy(1, dataSet, samples, 20);

    System.out.println("+ Small test ranges");
    testHistogramAccuracy(0, dataSet, samples, 5);
    testHistogramAccuracy(1, dataSet, samples, 5);
  }

  private double testHistogramAccuracy(int histogramType, List<Datum> dataSet, List<Datum> samples, int maxTestRange) {
    Histogram h;
    Datum from, to;
    Double estimate, real, error;
    Random r = new Random(System.currentTimeMillis());
    long startTime, elapsedTime;
    List<Double> relativeErrors = TUtil.newList();

    if (histogramType == 0) {
      h = new EquiWidthHistogram(Type.FLOAT8);
      System.out.println("  EquiWidthHistogram");
    } else {
      h = new EquiDepthHistogram(Type.FLOAT8);
      System.out.println("  EquiDepthHistogram");
    }

    startTime = System.nanoTime();
    h.construct(samples);
    elapsedTime = (System.nanoTime() - startTime) / 1000000;
    assertTrue(h.getIsReady() == true);

    double lower, upper;
    for (int i = 0; i < 1000; i++) {
      lower = r.nextDouble() + r.nextInt(maxBound) * 1.2;
      if (r.nextDouble() < 0.5) {
	lower = -lower;
      }
      upper = lower + r.nextDouble() * maxTestRange;

      from = DatumFactory.createFloat8(lower);
      to = DatumFactory.createFloat8(upper);
      
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

  private List<Datum> createShortDatumList1() {
    List<Datum> l = TUtil.newList();
    l.add(DatumFactory.createFloat8(0.6));
    l.add(DatumFactory.createFloat8(1.1));
    l.add(DatumFactory.createFloat8(1.25));
    l.add(DatumFactory.createFloat8(1.7));
    l.add(DatumFactory.createFloat8(1.9));
    l.add(DatumFactory.createFloat8(5.0));
    l.add(DatumFactory.createFloat8(5.0));
    l.add(DatumFactory.createFloat8(8.0));
    l.add(DatumFactory.createFloat8(9.0));
    l.add(DatumFactory.createFloat8(9.7));
    return l;
  }
  
  private List<Datum> createShortDatumList2() {
    List<Datum> l = TUtil.newList();
    l.add(DatumFactory.createFloat8(-6.0));
    l.add(DatumFactory.createFloat8(-5.1));
    l.add(DatumFactory.createFloat8(-1.25));
    l.add(DatumFactory.createFloat8(-1.17));
    l.add(DatumFactory.createFloat8(0.0));
    l.add(DatumFactory.createFloat8(3.2));
    l.add(DatumFactory.createFloat8(5.0));
    l.add(DatumFactory.createFloat8(8.0));
    l.add(DatumFactory.createFloat8(9.0));
    l.add(DatumFactory.createFloat8(9.7));
    return l;
  }
  
  private List<Datum> createShortDatumList3() {
    List<Datum> l = TUtil.newList();
    l.add(DatumFactory.createFloat8(-6.0));
    l.add(DatumFactory.createFloat8(-5.1));
    l.add(DatumFactory.createFloat8(-1.25));
    l.add(DatumFactory.createFloat8(-1.17));
    l.add(DatumFactory.createFloat8(0.0));
    l.add(DatumFactory.createFloat8(-3.2));
    l.add(DatumFactory.createFloat8(-5.0));
    l.add(DatumFactory.createFloat8(-8.0));
    l.add(DatumFactory.createFloat8(-9.0));
    l.add(DatumFactory.createFloat8(-9.7));
    return l;
  }
  
  @Test
  public final void testEquiWidthCase1() {
    List<Datum> samples = createShortDatumList1();
    EquiWidthHistogram h = new EquiWidthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 5);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == 1.9);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == 5);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 5);
  }

  @Test
  public final void testEquiWidthCase2() {
    List<Datum> samples = createShortDatumList2();
    EquiWidthHistogram h = new EquiWidthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == -5.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 3);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == -1.25);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 0.0);
    assertTrue(h.getBuckets().get(2).getFrequency() == 1);
    assertTrue(h.getBuckets().get(2).getMin().asFloat8() == 3.2);
    assertTrue(h.getBuckets().get(2).getMax().asFloat8() == 3.2);
    assertTrue(h.getBuckets().get(4).getFrequency() == 3);
    assertTrue(h.getBuckets().get(4).getMin().asFloat8() == 8.0);
    assertTrue(h.getBuckets().get(4).getMax().asFloat8() == 9.7);
  }

  @Test
  public final void testEquiWidthCase3() {
    List<Datum> samples = createShortDatumList3();
    EquiWidthHistogram h = new EquiWidthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 3);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == -9.7);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == -8.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 3);
    assertTrue(h.getBuckets().get(4).getMin().asFloat8() == -1.25);
    assertTrue(h.getBuckets().get(4).getMax().asFloat8() == 0.0);
  }

  @Test
  public final void testEquiDepthCase1() {
    List<Datum> samples = createShortDatumList1();
    EquiDepthHistogram h = new EquiDepthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == 1.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == 1.25);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 1.7);
    assertTrue(h.getBuckets().get(2).getFrequency() == 3);
    assertTrue(h.getBuckets().get(2).getMin().asFloat8() == 1.9);
    assertTrue(h.getBuckets().get(2).getMax().asFloat8() == 5.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 1);
    assertTrue(h.getBuckets().get(4).getMin().asFloat8() == 9.7);
    assertTrue(h.getBuckets().get(4).getMax().asFloat8() == 9.7);
  }

  @Test
  public final void testEquiDepthCase2() {
    List<Datum> samples = createShortDatumList1();
    samples.add(DatumFactory.createFloat8(9.8));
    EquiDepthHistogram h = new EquiDepthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 3);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 4);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == 1.7);
    assertTrue(h.getBuckets().get(1).getFrequency() == 4);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == 1.9);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 8.0);
    assertTrue(h.getBuckets().get(2).getFrequency() == 3);
    assertTrue(h.getBuckets().get(2).getMin().asFloat8() == 9.0);
    assertTrue(h.getBuckets().get(2).getMax().asFloat8() == 9.8);
  }

  @Test
  public final void testEquiDepthCase3() {
    List<Datum> samples = TUtil.newList();
    samples.add(DatumFactory.createFloat8(0.6));
    samples.add(DatumFactory.createFloat8(1.1));
    samples.add(DatumFactory.createFloat8(1.25));
    samples.add(DatumFactory.createFloat8(1.7));
    samples.add(DatumFactory.createFloat8(1.9));
    samples.add(DatumFactory.createFloat8(5.0));
    samples.add(DatumFactory.createFloat8(5.6));
    samples.add(DatumFactory.createFloat8(8.0));
    samples.add(DatumFactory.createFloat8(9.0));
    samples.add(DatumFactory.createFloat8(9.7));
    samples.add(DatumFactory.createFloat8(9.8));
    EquiDepthHistogram h = new EquiDepthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 2);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() <= 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 6);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == 0.6);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == 5.0);
    assertTrue(h.getBuckets().get(1).getFrequency() == 5);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == 5.6);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 9.8);
  }

  @Test
  public final void testEquiDepthCase4() {
    List<Datum> samples = createShortDatumList2();
    EquiDepthHistogram h = new EquiDepthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 5);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() == 5);
    assertTrue(h.getBuckets().get(0).getFrequency() == 2);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == -5.1);
    assertTrue(h.getBuckets().get(1).getFrequency() == 2);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == -1.25);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == -1.17);
    assertTrue(h.getBuckets().get(2).getFrequency() == 2);
    assertTrue(h.getBuckets().get(2).getMin().asFloat8() == 0.0);
    assertTrue(h.getBuckets().get(2).getMax().asFloat8() == 3.2);
    assertTrue(h.getBuckets().get(3).getFrequency() == 2);
    assertTrue(h.getBuckets().get(3).getMin().asFloat8() == 5.0);
    assertTrue(h.getBuckets().get(3).getMax().asFloat8() == 8.0);
    assertTrue(h.getBuckets().get(4).getFrequency() == 2);
    assertTrue(h.getBuckets().get(4).getMin().asFloat8() == 9.0);
    assertTrue(h.getBuckets().get(4).getMax().asFloat8() == 9.7);
  }

  @Test
  public final void testEquiDepthCase5() {
    List<Datum> samples = createShortDatumList2();
    EquiDepthHistogram h = new EquiDepthHistogram(Type.FLOAT8);
    assertTrue(h.getIsReady() == false);
    assertTrue(h.getBucketsCount() == 0);
    h.construct(samples, 3);

    assertTrue(h.getIsReady() == true);
    assertTrue(h.getBucketsCount() == 3);
    assertTrue(h.getBuckets().get(0).getFrequency() == 3);
    assertTrue(h.getBuckets().get(0).getMin().asFloat8() == -6.0);
    assertTrue(h.getBuckets().get(0).getMax().asFloat8() == -1.25);
    assertTrue(h.getBuckets().get(1).getFrequency() == 3);
    assertTrue(h.getBuckets().get(1).getMin().asFloat8() == -1.17);
    assertTrue(h.getBuckets().get(1).getMax().asFloat8() == 3.2);
    assertTrue(h.getBuckets().get(2).getFrequency() == 4);
    assertTrue(h.getBuckets().get(2).getMin().asFloat8() == 5.0);
    assertTrue(h.getBuckets().get(2).getMax().asFloat8() == 9.7);
  }

  @Test
  public final void testEqualsObjectEquiWidth() {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiWidthHistogram(Type.FLOAT8);
    h1.construct(samples);

    Histogram h2 = new Histogram(h1.getProto());
    assertEquals(h1, h2);
  }

  @Test
  public final void testEqualsObjectEquiDepth() {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiDepthHistogram(Type.FLOAT8);
    h1.construct(samples);

    Histogram h2 = new Histogram(h1.getProto());
    assertEquals(h1, h2);
  }

  @Test
  public final void testJsonEquiWidth() throws CloneNotSupportedException {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiWidthHistogram(Type.FLOAT8);
    h1.construct(samples);

    String json = h1.toJson();
    Histogram h2 = CatalogGsonHelper.fromJson(json, Histogram.class);
    assertEquals(h1, h2);
  }

  @Test
  public final void testJsonEquiDepth() throws CloneNotSupportedException {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiDepthHistogram(Type.FLOAT8);
    h1.construct(samples);

    String json = h1.toJson();
    Histogram h2 = CatalogGsonHelper.fromJson(json, Histogram.class);
    assertEquals(h1, h2);
  }

  @Test
  public final void testCloneEquiWidth() throws CloneNotSupportedException {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiWidthHistogram(Type.FLOAT8);
    h1.construct(samples);

    Histogram h2 = h1.clone();
    assertEquals(h1, h2);
  }

  @Test
  public final void testCloneEquiDepth() throws CloneNotSupportedException {
    List<Datum> samples = generateRandomData(Type.FLOAT8, 1000);
    Histogram h1 = new EquiDepthHistogram(Type.FLOAT8);
    h1.construct(samples);

    Histogram h2 = h1.clone();
    assertEquals(h1, h2);
  }
}
