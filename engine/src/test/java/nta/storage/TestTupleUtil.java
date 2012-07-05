package nta.storage;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.RangePartitionAlgorithm;
import nta.engine.planner.UniformRangePartition;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.utils.TupleUtil;
import org.junit.Test;
import tajo.worker.dataserver.HttpUtil;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTupleUtil {
  @Test
  public final void testToBytesAndToTuple() {
    Schema schema = new Schema();
    schema.addColumn("col1", DataType.BOOLEAN);
    schema.addColumn("col2", DataType.BYTE);
    schema.addColumn("col3", DataType.CHAR);
    schema.addColumn("col4", DataType.SHORT);
    schema.addColumn("col5", DataType.INT);
    schema.addColumn("col6", DataType.LONG);
    schema.addColumn("col7", DataType.FLOAT);
    schema.addColumn("col8", DataType.DOUBLE);
    schema.addColumn("col9", DataType.STRING);
    schema.addColumn("col10", DataType.BYTES);
    schema.addColumn("col11", DataType.IPv4);
    //schema.addColumn("col11", DataType.IPv6);
    
    Tuple tuple = new VTuple(11);
    tuple.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createChar('7'),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),        
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    });
    
    byte [] bytes = TupleUtil.toBytes(schema, tuple);
    Tuple tuple2 = TupleUtil.toTuple(schema, bytes);
    
    assertEquals(tuple, tuple2);
  }

  @Test
  public final void testGetPartitions() {
    Tuple sTuple = new VTuple(7);
    Tuple eTuple = new VTuple(7);

    Schema schema = new Schema();
    schema.addColumn("numByte", DataType.BYTE);
    schema.addColumn("numChar", DataType.CHAR);
    schema.addColumn("numShort", DataType.SHORT);
    schema.addColumn("numInt", DataType.INT);
    schema.addColumn("numLong", DataType.LONG);
    schema.addColumn("numFloat", DataType.FLOAT);
    schema.addColumn("numDouble", DataType.FLOAT);

    sTuple.put(0, DatumFactory.createByte((byte) 44));
    sTuple.put(1, DatumFactory.createChar('a'));
    sTuple.put(2, DatumFactory.createShort((short) 10));
    sTuple.put(3, DatumFactory.createInt(5));
    sTuple.put(4, DatumFactory.createLong(100));
    sTuple.put(5, DatumFactory.createFloat(100));
    sTuple.put(6, DatumFactory.createDouble(100));

    eTuple.put(0, DatumFactory.createByte((byte) 99));
    eTuple.put(1, DatumFactory.createChar('p'));
    eTuple.put(2, DatumFactory.createShort((short) 70));
    eTuple.put(3, DatumFactory.createInt(70));
    eTuple.put(4, DatumFactory.createLong(10000));
    eTuple.put(5, DatumFactory.createFloat(150));
    eTuple.put(6, DatumFactory.createDouble(170));

    RangePartitionAlgorithm partitioner = new UniformRangePartition(schema, new TupleRange(schema, sTuple, eTuple));
    TupleRange [] ranges = partitioner.partition(5);
    assertTrue(5 <= ranges.length);
    TupleComparator comp = new TupleComparator(schema, PlannerUtil.schemaToSortSpecs(schema));
    TupleRange prev = ranges[0];
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(comp.compare(prev.getStart(), ranges[i].getStart()) < 0);
      assertTrue(comp.compare(prev.getEnd(), ranges[i].getEnd()) < 0);
      prev = ranges[i];
    }
  }

  @Test
  public void testQueryToRange() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("intval", DataType.INT);
    schema.addColumn("floatval", DataType.FLOAT);

    Tuple s = new VTuple(2);
    s.put(0, DatumFactory.createInt(5));
    s.put(1, DatumFactory.createFloat(10));

    Tuple e = new VTuple(2);
    e.put(0, DatumFactory.createInt(10));
    e.put(1, DatumFactory.createFloat(20));

    TupleRange expected = new TupleRange(schema, s, e);
    int card = (int) TupleUtil.computeCardinality(schema, expected);
    assertEquals(66, card);
    int partNum = TupleUtil.getPartitions(schema, 5, expected).length;
    assertEquals(5, partNum);

    // TODO - partition should be improved to consider all fields
    //partNum = TupleUtil.partition(schema, 10, expected).length;
    //assertEquals(10, partNum);

    String query = TupleUtil.rangeToQuery(schema, expected, false);

    TupleRange range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);

    query = TupleUtil.rangeToQuery(schema, expected, true);
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    assertTrue(params.containsKey("final"));
    range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);
  }

  @Test
  public void testQueryToRangeWithOneRange() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("partkey", DataType.FLOAT);

    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createFloat(28082));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createFloat(28082));

    TupleRange expected = new TupleRange(schema, s, e);
    int card = (int) TupleUtil.computeCardinality(schema, expected);
    assertEquals(1, card);
    int partNum = TupleUtil.getPartitions(schema, card, expected).length;
    assertEquals(1, partNum);

    String query = TupleUtil.rangeToQuery(schema, expected, false);

    TupleRange range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);

    query = TupleUtil.rangeToQuery(schema, expected, true);
    Map<String,String> params = HttpUtil.getParamsFromQuery(query);
    assertTrue(params.containsKey("final"));
    range = TupleUtil.queryToRange(schema, query);
    assertEquals(expected, range);
  }

  @Test
  /**
   * It verifies NTA-805.
   */
  public void testRangeToQueryHeavyTest() throws UnsupportedEncodingException {
    Schema schema = new Schema();
    schema.addColumn("c_custkey", DataType.INT);
    Tuple s = new VTuple(1);
    s.put(0, DatumFactory.createInt(4));
    Tuple e = new VTuple(1);
    e.put(0, DatumFactory.createInt(149995));
    TupleRange expected = new TupleRange(schema, s, e);
    TupleRange [] ranges = TupleUtil.getPartitions(schema, 31, expected);

    String query;
    for (TupleRange range : ranges) {
      query = TupleUtil.rangeToQuery(schema, range, false);
      TupleRange result = TupleUtil.queryToRange(schema, query);
      assertEquals(range, result);
    }
  }

  @Test
  /**
   * It verifies NTA-807
   */
  public void testRangeToQueryTest() throws UnsupportedEncodingException {
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

    RangePartitionAlgorithm partitioner = new UniformRangePartition(schema, expected, true);
    TupleRange [] ranges = partitioner.partition(31);

    String query;
    for (TupleRange range : ranges) {
      query = TupleUtil.rangeToQuery(schema, range, false);
      TupleRange result = TupleUtil.queryToRange(schema, query);
      assertEquals(range, result);
    }
  }
}
