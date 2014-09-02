/***
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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.storage.offheap.TestRowOrientedRowBlock;
import org.apache.tajo.storage.rawfile.DirectRawFileScanner;
import org.junit.Test;

import java.io.IOException;

import static org.apache.tajo.storage.offheap.TestRowOrientedRowBlock.schema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestPairWiseMerger {
  static TajoConf conf;
  private static TupleComparatorImpl comparator;
  static {
    conf = new TajoConf();

    comparator = new TupleComparatorImpl(TestRowOrientedRowBlock.schema,
        new SortSpec[] {
            new SortSpec(new Column("col2", TajoDataTypes.Type.INT4)),
            new SortSpec(new Column("col3", TajoDataTypes.Type.INT8))
        });
  }

  @Test
  public void testPairWiseMergerWithTwoLeaves() throws IOException {
    int [] rowNums = new int [] {500, 1000};

    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves1() throws IOException {
    int [] rowNums = new int[] {1, 1, 1};
    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves2() throws IOException {
    int [] rowNums = new int[] {0, 0, 1};
    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves3() throws IOException {
    int [] rowNums = new int[] {0, 1, 0};
    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves4() throws IOException {
    int [] rowNums = new int[] {1, 0, 0};
    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves5() throws IOException {
    int [] rowNums = new int[] {1, 0, 1};
    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves6() throws IOException {
    int [] rowNums = new int[] {1, 1, 0};

    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testPairWiseMergerWithThreeLeaves7() throws IOException {
    int [] rowNums = new int[] {1, 0, 0};

    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  @Test
  public void testThreeLevelPairWiseMerger1() throws IOException {
    int [] rowNums = new int[] {500, 501, 499, 498, 489, 450, 431, 429};

    Scanner [] scanners = createScanners(rowNums);

    PairWiseMerger merger = createLeftDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();

    merger = createRightDeepMerger(scanners);

    merger.init();
    assertSortResult(rowNums, merger, comparator);
    merger.reset();
    assertSortResult(rowNums, merger, comparator);
    merger.close();
  }

  private static PairWiseMerger createLeftDeepMerger(Scanner [] scanners) throws IOException {
    PairWiseMerger prev = null;
    for (int i = 1; i < scanners.length; i++) {

      if (i == 1) {
        prev = new PairWiseMerger(schema, scanners[i - 1], scanners[i], comparator); // initial one
      } else {
        prev = new PairWiseMerger(schema, prev, scanners[i], comparator);
      }
    }

    return prev;
  }

  private static PairWiseMerger createRightDeepMerger(Scanner [] scanners) throws IOException {
    PairWiseMerger prev = null;
    for (int i = 1; i < scanners.length; i++) {

      if (i == 1) {
        prev = new PairWiseMerger(schema, scanners[i - 1], scanners[i], comparator); // initial one
      } else {
        prev = new PairWiseMerger(schema, scanners[i], prev, comparator);
      }
    }

    return prev;
  }

  private static  Scanner [] createScanners(int [] rowNums) throws IOException {
    DirectRawFileScanner[] scanners = new DirectRawFileScanner[rowNums.length];

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.DIRECTRAW);
    for (int i = 0; i < rowNums.length; i++) {
      scanners[i] = TestExternalSortExec.createSortedScanner(conf, meta, rowNums[i], comparator);
    }

    assertEquals(rowNums.length, scanners.length);
    return scanners;
  }

  private static void assertSortResult(int[] rowNums, Scanner scanner, TupleComparatorImpl comparator) throws IOException {
    Tuple tuple;
    Tuple curVal;
    Tuple preVal = null;
    int idx = 0;
    while ((tuple = scanner.next()) != null) {
      curVal = tuple;
      if (preVal != null) {
        assertTrue(idx + "th, prev: " + preVal + ", but cur: " + curVal, comparator.compare(preVal, curVal) <= 0);
      }
      preVal = curVal;
      idx++;
    }

    int totalRowNum = 0;
    for (int i = 0; i < rowNums.length; i++) {
      totalRowNum += rowNums[i];
    }
    assertEquals(totalRowNum, idx);
  }
}