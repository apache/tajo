/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.planner;

import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.datum.DatumFactory;
import tajo.engine.utils.TupleUtil;
import tajo.storage.Tuple;
import tajo.storage.TupleRange;
import tajo.storage.VTuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestUniformRangePartition {
  /**
   * It verify overflow and increment.
   */
  @Test
  public void testIncrement1() {
    Schema schema = new Schema()
    .addColumn("l_returnflag", DataType.STRING)
    .addColumn("l_linestatus", DataType.STRING);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createString("A"));
    s.put(1, DatumFactory.createString("A"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createString("D"));
    e.put(1, DatumFactory.createString("C"));

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner =
        new UniformRangePartition(schema, expected);
    assertEquals(12, TupleUtil.computeCardinality(schema, expected));

    String [] result = new String[12];
    result[0] = "AA";
    result[1] = "AB";
    result[2] = "AC";
    result[3] = "BA";
    result[4] = "BB";
    result[5] = "BC";
    result[6] = "CA";
    result[7] = "CB";
    result[8] = "CC";
    result[9] = "DA";
    result[10] = "DB";
    result[11] = "DC";

    Tuple end = partitioner.increment(s, 1, 1);
    assertEquals("A", end.get(0).asChars());
    assertEquals("B", end.get(1).asChars());
    for (int i = 2; i < 11; i++ ) {
      end = partitioner.increment(end, 1, 1);
      assertEquals(result[i].charAt(0), end.get(0).asChars().charAt(0));
      assertEquals(result[i].charAt(1), end.get(1).asChars().charAt(0));
    }
  }

  /**
   * It verify overflow with the number that exceeds the last digit.
   */
  @Test
  public void testIncrement2() {
    Schema schema = new Schema()
    .addColumn("l_returnflag", DataType.STRING)
    .addColumn("l_linestatus", DataType.STRING);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createString("A"));
    s.put(1, DatumFactory.createString("A"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createString("D"));
    e.put(1, DatumFactory.createString("C"));

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner =
        new UniformRangePartition(schema, expected);
    assertEquals(12, TupleUtil.computeCardinality(schema, expected));

    String [] result = new String[12];
    result[0] = "AA";
    result[1] = "AB";
    result[2] = "AC";
    result[3] = "BA";
    result[4] = "BB";
    result[5] = "BC";
    result[6] = "CA";
    result[7] = "CB";
    result[8] = "CC";
    result[9] = "DA";
    result[10] = "DB";
    result[11] = "DC";

    Tuple end = partitioner.increment(s, 6, 1);
    assertEquals("C", end.get(0).asChars());
    assertEquals("A", end.get(1).asChars());
    end = partitioner.increment(end, 5, 1);
    assertEquals("D", end.get(0).asChars());
    assertEquals("C", end.get(1).asChars());
  }

  /**
   * It verify the case where two or more digits are overflow.
   */
  @Test
  public void testIncrement3() {
    Schema schema = new Schema()
    .addColumn("l_returnflag", DataType.STRING)
    .addColumn("l_linestatus", DataType.STRING)
    .addColumn("final", DataType.STRING);

    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createString("A"));
    s.put(1, DatumFactory.createString("A"));
    s.put(2, DatumFactory.createString("A"));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createString("D")); //  4
    e.put(1, DatumFactory.createString("B")); //  2
    e.put(2, DatumFactory.createString("C")); // x3 = 24

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner =
        new UniformRangePartition(schema, expected);
    assertEquals(24, TupleUtil.computeCardinality(schema, expected));

    Tuple overflowBefore = partitioner.increment(s, 5, 2);
    assertEquals("A", overflowBefore.get(0).asChars());
    assertEquals("B", overflowBefore.get(1).asChars());
    assertEquals("C", overflowBefore.get(2).asChars());
    Tuple overflowed = partitioner.increment(overflowBefore, 1, 2);
    assertEquals("B", overflowed.get(0).asChars());
    assertEquals("A", overflowed.get(1).asChars());
    assertEquals("A", overflowed.get(2).asChars());
  }

  @Test
  public void testIncrement4() {
    Schema schema = new Schema()
    .addColumn("l_orderkey", DataType.LONG)
    .addColumn("l_linenumber", DataType.LONG);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createLong(10));
    s.put(1, DatumFactory.createLong(20));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createLong(19));
    e.put(1, DatumFactory.createLong(39));

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner =
        new UniformRangePartition(schema, expected);
    assertEquals(200, partitioner.getTotalCardinality().longValue());

    Tuple range2 = partitioner.increment(s, 100, 1);
    assertEquals(15, range2.get(0).asInt());
    assertEquals(20, range2.get(1).asInt());
    Tuple range3 = partitioner.increment(range2, 99, 1);
    assertEquals(19, range3.get(0).asInt());
    assertEquals(39, range3.get(1).asInt());
  }

  @Test public void testIncrement5() {
    Schema schema = new Schema()
    .addColumn("l_orderkey", DataType.LONG)
    .addColumn("l_linenumber", DataType.LONG)
    .addColumn("final", DataType.LONG);
    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createLong(1));
    s.put(1, DatumFactory.createLong(1));
    s.put(2, DatumFactory.createLong(1));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createLong(4)); // 4
    e.put(1, DatumFactory.createLong(2)); // 2
    e.put(2, DatumFactory.createLong(3)); //x3 = 24

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner
        = new UniformRangePartition(schema, expected);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, 5, 2);
    assertEquals(1, beforeOverflow.get(0).asLong());
    assertEquals(2, beforeOverflow.get(1).asLong());
    assertEquals(3, beforeOverflow.get(2).asLong());
    Tuple overflow = partitioner.increment(beforeOverflow, 1, 2);
    assertEquals(2, overflow.get(0).asLong());
    assertEquals(1, overflow.get(1).asLong());
    assertEquals(1, overflow.get(2).asLong());
  }

  @Test
  public void testIncrement6() {
    Schema schema = new Schema()
      .addColumn("l_orderkey", DataType.DOUBLE)
      .addColumn("l_linenumber", DataType.DOUBLE)
      .addColumn("final", DataType.DOUBLE);
    Tuple s = new VTuple(3);
    s.put(0, DatumFactory.createDouble(1.1d));
    s.put(1, DatumFactory.createDouble(1.1d));
    s.put(2, DatumFactory.createDouble(1.1d));
    Tuple e = new VTuple(3);
    e.put(0, DatumFactory.createDouble(4.1d)); // 4
    e.put(1, DatumFactory.createDouble(2.1d)); // 2
    e.put(2, DatumFactory.createDouble(3.1d)); //x3 = 24

    TupleRange expected = new TupleRange(schema, s, e);

    UniformRangePartition partitioner =
        new UniformRangePartition(schema, expected);
    assertEquals(24, partitioner.getTotalCardinality().longValue());

    Tuple beforeOverflow = partitioner.increment(s, 5, 2);
    assertTrue(1.1d == beforeOverflow.get(0).asDouble());
    assertTrue(2.1d == beforeOverflow.get(1).asDouble());
    assertTrue(3.1d == beforeOverflow.get(2).asDouble());
    Tuple overflow = partitioner.increment(beforeOverflow, 1, 2);
    assertTrue(2.1d == overflow.get(0).asDouble());
    assertTrue(1.1d == overflow.get(1).asDouble());
    assertTrue(1.1d == overflow.get(2).asDouble());
  }

  @Test
  public void testPartition() {
    Schema schema = new Schema();
    schema.addColumn("l_returnflag", DataType.STRING);
    schema.addColumn("l_linestatus", DataType.STRING);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createString("A"));
    s.put(1, DatumFactory.createString("F"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createString("R"));
    e.put(1, DatumFactory.createString("O"));
    TupleRange expected = new TupleRange(schema, s, e);
    RangePartitionAlgorithm partitioner
        = new UniformRangePartition(schema, expected, true);
    TupleRange [] ranges = partitioner.partition(31);


    TupleRange prev = null;
    for (TupleRange r : ranges) {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0);
      }
    }
  }

  @Test
  public void testPartitionForOnePartNum() {
    Schema schema = new Schema()
      .addColumn("l_returnflag", DataType.STRING)
      .addColumn("l_linestatus", DataType.STRING);
    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createString("A"));
    s.put(1, DatumFactory.createString("F"));
    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createString("R"));
    e.put(1, DatumFactory.createString("O"));
    TupleRange expected = new TupleRange(schema, s, e);
    RangePartitionAlgorithm partitioner =
        new UniformRangePartition(schema, expected, true);
    TupleRange [] ranges = partitioner.partition(1);

    assertEquals(expected, ranges[0]);
  }
}
