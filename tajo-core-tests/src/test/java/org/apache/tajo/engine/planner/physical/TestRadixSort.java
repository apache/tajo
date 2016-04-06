package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaFactory;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
  private static final int tupleNum = 10;
  private static final Random random = new Random(System.currentTimeMillis());
  private SortSpec[] sortSpecs;
  private final static Datum MINUS_ONE = DatumFactory.createInt4(-1);

  static {
    queryContext = new QueryContext(new TajoConf());
    queryContext.setInt(SessionVars.TEST_TIM_SORT_THRESHOLD_FOR_RADIX_SORT, 0);

    schema = SchemaFactory.newV1();
    schema.addColumn("col0", Type.INT8);
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.INT2);
    schema.addColumn("col3", Type.DATE);
    schema.addColumn("col4", Type.TIMESTAMP);
    schema.addColumn("col5", Type.TIME);
    schema.addColumn("col6", Type.INET4);
    schema.addColumn("col7", Type.FLOAT4);
    schema.addColumn("col8", Type.FLOAT8);
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

    params.add(new Object[] {
        new Param(new SortSpec[] {
            new SortSpec(schema.getColumn(7), true, false)
        })
    });

    // Test every single column sort
//    for (int i = 0; i < schema.size(); i++) {
//      params.add(new Object[] {
//          new Param(
//              new SortSpec[] {
//                  new SortSpec(schema.getColumn(i), random.nextBoolean(), random.nextBoolean())
//              })
//      });
//    }

    // Randomly choose columns
//    for (int colNum = 2; colNum < 6; colNum++) {
//      for (int i =0; i < 5; i++) {
//        SortSpec[] sortSpecs = new SortSpec[colNum];
//        for (int j = 0; j <colNum; j++) {
//          sortSpecs[j] = new SortSpec(schema.getColumn(random.nextInt(schema.size())),
//              random.nextBoolean(), random.nextBoolean());
//        }
//        params.add(new Object[] {new Param(sortSpecs)});
//      }
//    }

    return params;
  }

  @BeforeClass
  public static void setup() {
    List<DataType> dataTypeList = schema.getRootColumns().stream().map(c -> c.getDataType()).collect(Collectors.toList());
    tuples = new UnSafeTupleList(dataTypeList.toArray(new DataType[dataTypeList.size()]), tupleNum);

    // add null and negative numbers
    VTuple tuple = new VTuple(schema.size());
    IntStream.range(0, tupleNum).forEach(i -> {
      // Null tuple ratio: 10%
      if (random.nextInt(10) == 0) {
        makeNullTuple(tuple);
      } else {
        makeRandomTuple(tuple);
      }
      tuples.addTuple(tuple);
    });
  }

  @AfterClass
  public static void teardown() {
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
        DatumFactory.createDate(random.nextInt(2147483647)),
        DatumFactory.createTimestamp(random.nextInt(9999), random.nextInt(12) + 1, random.nextInt(30),
            random.nextInt(24) + 1, random.nextInt(60), random.nextInt(60), 0),
        DatumFactory.createTime(random.nextInt(24) + 1, random.nextInt(60), random.nextInt(60), 0),
        DatumFactory.createInet4(random.nextInt()),
        DatumFactory.createFloat4(random.nextFloat()),
        DatumFactory.createFloat8(random.nextDouble())
    });

    for (int i = 0; i < 3; i++) {
      if (random.nextBoolean()) {
        tuple.put(i, tuple.asDatum(i).multiply(MINUS_ONE));
      }
    }

    for (int i = 7; i < 9; i++) {
      if (random.nextBoolean()) {
        tuple.put(i, tuple.asDatum(i).multiply(MINUS_ONE));
      }
    }

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
