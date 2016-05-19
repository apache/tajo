/*
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

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaBuilder;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.engine.planner.physical.ExternalSortExec.UnSafeComparator;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.tuple.memory.UnSafeTupleList;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestRadixSort {
  private final static QueryContext queryContext;
  private static UnSafeTupleList tuples;
  private static Schema schema;
  private static final int tupleNum = 1000;
  private static final Random random = new Random(System.currentTimeMillis());
  private SortSpec[] sortSpecs;
  private final static Datum MINUS_ONE = DatumFactory.createInt4(-1);

  static {
    queryContext = new QueryContext(new TajoConf());
    queryContext.setInt(SessionVars.TEST_TIM_SORT_LIMIT_FOR_RADIX_SORT, 0);

    schema = SchemaBuilder.builder().addAll(new Column[]{
        new Column("col0", Type.INT8),
        new Column("col1", Type.INT4),
        new Column("col2", Type.INT2),
        new Column("col3", Type.DATE),
        new Column("col4", Type.TIMESTAMP),
        new Column("col5", Type.TIME),
        new Column("col6", Type.FLOAT4),
        new Column("col7", Type.FLOAT8)
    }).build();
  }

  private static class Param {
    final SortSpec[] sortSpecs;

    public Param(SortSpec[] param) {
      this.sortSpecs = param;
    }

    @Override
    public String toString() {
      return StringUtils.join(sortSpecs);
    }
  }

  public TestRadixSort(Param param) {
    this.sortSpecs = param.sortSpecs;
  }

  @Parameters(name = "{index}: {0}")
  public static Collection<Object[]> generateParameters() {
    List<Object[]> params = new ArrayList<>();

    // Test every single column sort
    for (int i = 0; i < schema.size(); i++) {
      params.add(new Object[] {
          new Param(
              new SortSpec[] {
                  new SortSpec(schema.getColumn(i), random.nextBoolean(), random.nextBoolean())
              })
      });
    }

    // Randomly choose columns
    for (int colNum = 2; colNum < 6; colNum++) {
      for (int i =0; i < 5; i++) {
        SortSpec[] sortSpecs = new SortSpec[colNum];
        for (int j = 0; j <colNum; j++) {
          sortSpecs[j] = new SortSpec(schema.getColumn(random.nextInt(schema.size())),
              random.nextBoolean(), random.nextBoolean());
        }
        params.add(new Object[] {new Param(sortSpecs)});
      }
    }

    return params;
  }

  @Before
  public void setup() {
    List<DataType> dataTypeList = schema.getRootColumns().stream().map(Column::getDataType).collect(Collectors.toList());
    tuples = new UnSafeTupleList(dataTypeList.toArray(new DataType[dataTypeList.size()]), tupleNum);

    // add null and negative numbers
    VTuple tuple = new VTuple(schema.size());
    IntStream.range(0, tupleNum - 6).forEach(i -> {
      // Each of null tuples, max tuples, and min tuples occupies 10 % of the total tuples.
      int r = random.nextInt(10);
      switch (r) {
        case 0:
          makeNullTuple(tuple);
          break;
        case 1:
          makeMaxTuple(tuple);
          break;
        case 2:
          makeMinTuple(tuple);
          break;
        default:
          makeRandomTuple(tuple);
          break;
      }

      tuples.addTuple(tuple);
    });

    // Add at least 2 null, max, min tuples.
    makeMaxTuple(tuple);
    tuples.addTuple(tuple);
    makeMinTuple(tuple);
    tuples.addTuple(tuple);
    makeNullTuple(tuple);
    tuples.addTuple(tuple);
    makeMaxTuple(tuple);
    tuples.addTuple(tuple);
    makeMinTuple(tuple);
    tuples.addTuple(tuple);
    makeNullTuple(tuple);
    tuples.addTuple(tuple);
  }

  @After
  public void teardown() {
    tuples.release();
  }

  private static Tuple makeNullTuple(Tuple tuple) {
    tuple.put(new Datum[] {
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get(),
        NullDatum.get()
    });
    return tuple;
  }

  private static Tuple makeRandomTuple(Tuple tuple) {
    tuple.put(new Datum[]{
        DatumFactory.createInt8(random.nextLong()),
        DatumFactory.createInt4(random.nextInt()),
        DatumFactory.createInt2((short) random.nextInt(Short.MAX_VALUE)),
        DatumFactory.createDate(Math.abs(random.nextInt())),
        DatumFactory.createTimestamp(Math.abs(random.nextLong())),
        DatumFactory.createTime(Math.abs(random.nextLong())),
        DatumFactory.createFloat4(random.nextFloat()),
        DatumFactory.createFloat8(random.nextDouble())
    });

    for (int i = 0; i < 3; i++) {
      if (random.nextBoolean()) {
        tuple.put(i, tuple.asDatum(i).multiply(MINUS_ONE));
      }
    }

    for (int i = 6; i < 8; i++) {
      if (random.nextBoolean()) {
        tuple.put(i, tuple.asDatum(i).multiply(MINUS_ONE));
      }
    }

    return tuple;
  }

  private static Tuple makeMaxTuple(Tuple tuple) {
    tuple.put(new Datum[]{
        DatumFactory.createInt8(Long.MAX_VALUE),
        DatumFactory.createInt4(Integer.MAX_VALUE),
        DatumFactory.createInt2(Short.MAX_VALUE),
        DatumFactory.createDate(Integer.MAX_VALUE),
        DatumFactory.createTimestamp(
            // FIXME 'Out of Range of Time'
            //DateTimeUtil.toJulianDate(JULIAN_MAXYEAR, 1, 1)
            DateTimeUtil.toJulianTimestamp(DateTimeConstants.JULIAN_MAXYEAR / 20, 1, 1, 0, 0, 0, 0)),
        DatumFactory.createTime(Long.MAX_VALUE),
        DatumFactory.createFloat4(Float.MAX_VALUE),
        DatumFactory.createFloat8(Double.MAX_VALUE)
    });

    return tuple;
  }

  private static Tuple makeMinTuple(Tuple tuple) {
    tuple.put(new Datum[]{
        DatumFactory.createInt8(Long.MIN_VALUE),
        DatumFactory.createInt4(Integer.MIN_VALUE),
        DatumFactory.createInt2(Short.MIN_VALUE),
        DatumFactory.createDate(0),
        DatumFactory.createTimestamp(0),
        DatumFactory.createTime(0),
        DatumFactory.createFloat4(Float.MIN_VALUE),
        DatumFactory.createFloat8(Double.MIN_VALUE)
    });

    return tuple;
  }

  @Test
  public void testSort() {
    Comparator<UnSafeTuple> comparator = new UnSafeComparator(schema, sortSpecs);

    RadixSort.sort(queryContext, tuples, schema, sortSpecs, comparator);

    IntStream.range(0, tuples.size() - 1)
        .forEach(i -> {
          assertTrue(tuples.get(i) + " precedes " + tuples.get(i + 1) + " at " + i,
              comparator.compare(tuples.get(i), tuples.get(i + 1)) <= 0);
        });
  }
}
