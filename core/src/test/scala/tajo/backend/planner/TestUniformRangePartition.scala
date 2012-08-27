package tajo.backend.planner

import org.scalatest.junit.AssertionsForJUnit
import org.junit.Test
import org.junit.Assert._
import tajo.catalog.proto.CatalogProtos.DataType
import tajo.storage.{TupleRange, VTuple, Tuple}
import tajo.engine.planner.{RangePartitionAlgorithm, UniformRangePartition}
import tajo.datum.DatumFactory
import tajo.catalog.Schema
import tajo.engine.utils.TupleUtil

/**
 * @author Hyunsik Choi
 */

class TestUniformRangePartition extends AssertionsForJUnit {
  /**
   * It verify overflow and increment.
   */
  @Test def testIncrement1() {
    val schema: Schema = new Schema
    schema.addColumn("l_returnflag", DataType.STRING)
    schema.addColumn("l_linestatus", DataType.STRING)
    val s: Tuple = new VTuple(2)
    s.put(0, DatumFactory.createString("A"))
    s.put(1, DatumFactory.createString("A"))
    val e: Tuple = new VTuple(2)
    e.put(0, DatumFactory.createString("D"))
    e.put(1, DatumFactory.createString("C"))

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(12, TupleUtil.computeCardinality(schema, expected))

    var result = new Array[String](12)
    result(0) = "AA"
    result(1) = "AB"
    result(2) = "AC"
    result(3) = "BA"
    result(4) = "BB"
    result(5) = "BC"
    result(6) = "CA"
    result(7) = "CB"
    result(8) = "CC"
    result(9) = "DA"
    result(10) = "DB"
    result(11) = "DC"

    var end = partitioner.increment(s, 1, 1);
    assertEquals("A", end.get(0).asChars())
    assertEquals("B", end.get(1).asChars())
    for (i <- 2 until 11) {
      end = partitioner.increment(end, 1, 1);
      assertEquals(result(i).charAt(0), end.get(0).asChars().charAt(0))
      assertEquals(result(i).charAt(1), end.get(1).asChars().charAt(0))
    }
  }

  /**
   * It verify overflow with the number that exceeds the last digit.
   */
  @Test def testIncrement2() {
    val schema: Schema = new Schema
    schema.addColumn("l_returnflag", DataType.STRING)
    schema.addColumn("l_linestatus", DataType.STRING)
    val s: Tuple = new VTuple(2)
    s.put(0, DatumFactory.createString("A"))
    s.put(1, DatumFactory.createString("A"))
    val e: Tuple = new VTuple(2)
    e.put(0, DatumFactory.createString("D"))
    e.put(1, DatumFactory.createString("C"))

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(12, TupleUtil.computeCardinality(schema, expected))

    var result = new Array[String](12)
    result(0) = "AA"
    result(1) = "AB"
    result(2) = "AC"
    result(3) = "BA"
    result(4) = "BB"
    result(5) = "BC"
    result(6) = "CA"
    result(7) = "CB"
    result(8) = "CC"
    result(9) = "DA"
    result(10) = "DB"
    result(11) = "DC"

    var end = partitioner.increment(s, 6, 1);
    assertEquals("C", end.get(0).asChars())
    assertEquals("A", end.get(1).asChars())
    end = partitioner.increment(end, 5, 1);
    assertEquals("D", end.get(0).asChars())
    assertEquals("C", end.get(1).asChars())
  }

  /**
   * It verify the case where two or more digits are overflow.
   */
  @Test def testIncrement3() {
    val schema: Schema = new Schema
    schema.addColumn("l_returnflag", DataType.STRING)
    schema.addColumn("l_linestatus", DataType.STRING)
    schema.addColumn("final", DataType.STRING)
    val s: Tuple = new VTuple(3)
    s.put(0, DatumFactory.createString("A"))
    s.put(1, DatumFactory.createString("A"))
    s.put(2, DatumFactory.createString("A"))
    val e: Tuple = new VTuple(3)
    e.put(0, DatumFactory.createString("D")) //  4
    e.put(1, DatumFactory.createString("B")) //  2
    e.put(2, DatumFactory.createString("C")) // x3 = 24

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(24, TupleUtil.computeCardinality(schema, expected))

    var overflowBefore = partitioner.increment(s, 5, 2);
    assertEquals("A", overflowBefore.get(0).asChars())
    assertEquals("B", overflowBefore.get(1).asChars())
    assertEquals("C", overflowBefore.get(2).asChars())
    var overflowed = partitioner.increment(overflowBefore, 1, 2);
    assertEquals("B", overflowed.get(0).asChars())
    assertEquals("A", overflowed.get(1).asChars())
    assertEquals("A", overflowed.get(2).asChars())
  }

  @Test def testIncrement4() {
    val schema: Schema = new Schema
    schema.addColumn("l_orderkey", DataType.LONG)
    schema.addColumn("l_linenumber", DataType.LONG)
    val s: Tuple = new VTuple(2)
    s.put(0, DatumFactory.createLong(10))
    s.put(1, DatumFactory.createLong(20))
    val e: Tuple = new VTuple(2)
    e.put(0, DatumFactory.createLong(19))
    e.put(1, DatumFactory.createLong(39))

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(200, partitioner.getTotalCardinality.longValue())

    var range2 = partitioner.increment(s, 100, 1)
    assertEquals(15, range2.get(0).asInt())
    assertEquals(20, range2.get(1).asInt())
    var range3 = partitioner.increment(range2, 99, 1)
    assertEquals(19, range3.get(0).asInt())
    assertEquals(39, range3.get(1).asInt())
  }

  @Test def testIncrement5() {
    val schema: Schema = new Schema
    schema.addColumn("l_orderkey", DataType.LONG)
    schema.addColumn("l_linenumber", DataType.LONG)
    schema.addColumn("final", DataType.LONG)
    val s: Tuple = new VTuple(3)
    s.put(0, DatumFactory.createLong(1))
    s.put(1, DatumFactory.createLong(1))
    s.put(2, DatumFactory.createLong(1))
    val e: Tuple = new VTuple(3)
    e.put(0, DatumFactory.createLong(4)) // 4
    e.put(1, DatumFactory.createLong(2)) // 2
    e.put(2, DatumFactory.createLong(3)) //x3 = 24

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(24, partitioner.getTotalCardinality.longValue())

    var beforeOverflow = partitioner.increment(s, 5, 2)
    assertEquals(1, beforeOverflow.get(0).asLong())
    assertEquals(2, beforeOverflow.get(1).asLong())
    assertEquals(3, beforeOverflow.get(2).asLong())
    var overflow = partitioner.increment(beforeOverflow, 1, 2)
    assertEquals(2, overflow.get(0).asLong())
    assertEquals(1, overflow.get(1).asLong())
    assertEquals(1, overflow.get(2).asLong())
  }

  @Test def testIncrement6() {
    val schema: Schema = new Schema
    schema.addColumn("l_orderkey", DataType.DOUBLE)
    schema.addColumn("l_linenumber", DataType.DOUBLE)
    schema.addColumn("final", DataType.DOUBLE)
    val s: Tuple = new VTuple(3)
    s.put(0, DatumFactory.createDouble(1.1d))
    s.put(1, DatumFactory.createDouble(1.1d))
    s.put(2, DatumFactory.createDouble(1.1d))
    val e: Tuple = new VTuple(3)
    e.put(0, DatumFactory.createDouble(4.1d)) // 4
    e.put(1, DatumFactory.createDouble(2.1d)) // 2
    e.put(2, DatumFactory.createDouble(3.1d)) //x3 = 24

    val expected: TupleRange = new TupleRange(schema, s, e)

    var partitioner = new UniformRangePartition(schema, expected)
    assertEquals(24, partitioner.getTotalCardinality.longValue())

    var beforeOverflow = partitioner.increment(s, 5, 2)
    assertTrue(1.1d == beforeOverflow.get(0).asDouble())
    assertTrue(2.1d == beforeOverflow.get(1).asDouble())
    assertTrue(3.1d == beforeOverflow.get(2).asDouble())
    var overflow = partitioner.increment(beforeOverflow, 1, 2)
    assertTrue(2.1d == overflow.get(0).asDouble())
    assertTrue(1.1d == overflow.get(1).asDouble())
    assertTrue(1.1d == overflow.get(2).asDouble())
  }

  @Test def testPartition() {
    val schema: Schema = new Schema
    schema.addColumn("l_returnflag", DataType.STRING)
    schema.addColumn("l_linestatus", DataType.STRING)
    val s: Tuple = new VTuple(2)
    s.put(0, DatumFactory.createString("A"))
    s.put(1, DatumFactory.createString("F"))
    val e: Tuple = new VTuple(2)
    e.put(0, DatumFactory.createString("R"))
    e.put(1, DatumFactory.createString("O"))
    val expected: TupleRange = new TupleRange(schema, s, e)
    val partitioner: RangePartitionAlgorithm = new UniformRangePartition(schema, expected, true)
    val ranges: Array[TupleRange] = partitioner.partition(31)


    var prev : TupleRange = null
    ranges.foreach(r => {
      if (prev == null) {
        prev = r;
      } else {
        assertTrue(prev.compareTo(r) < 0)
      }
    })
  }

  @Test def testPartitionForOnePartNum() {
    val schema: Schema = new Schema
    schema.addColumn("l_returnflag", DataType.STRING)
    schema.addColumn("l_linestatus", DataType.STRING)
    val s: Tuple = new VTuple(2)
    s.put(0, DatumFactory.createString("A"))
    s.put(1, DatumFactory.createString("F"))
    val e: Tuple = new VTuple(2)
    e.put(0, DatumFactory.createString("R"))
    e.put(1, DatumFactory.createString("O"))
    val expected: TupleRange = new TupleRange(schema, s, e)
    val partitioner: RangePartitionAlgorithm = new UniformRangePartition(schema, expected, true)
    val ranges: Array[TupleRange] = partitioner.partition(1)

    assertEquals(expected, ranges(0));
  }
}
