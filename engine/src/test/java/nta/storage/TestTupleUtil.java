package nta.storage;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.engine.planner.PlannerUtil;
import nta.engine.planner.physical.TupleComparator;
import nta.engine.utils.TupleUtil;
import org.junit.Test;

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

    TupleRange [] ranges = TupleUtil.getPartitions(schema, 5, sTuple, eTuple);
    assertEquals(5, ranges.length);
    TupleComparator comp = new TupleComparator(schema, PlannerUtil.schemaToSortSpecs(schema));
    TupleRange prev = ranges[0];
    for (int i = 1; i < ranges.length; i++) {
      assertTrue(comp.compare(prev.getStart(), ranges[i].getStart()) < 0);
      assertTrue(comp.compare(prev.getEnd(), ranges[i].getEnd()) < 0);
      prev = ranges[i];
    }
  }
}
