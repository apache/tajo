package nta.storage;

import static org.junit.Assert.assertEquals;
import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.DatumFactory;

import org.junit.Test;

public class TestTupleUtil {
  @Test
  public final void testToBytesAndToTuple() {
    Schema schema = new Schema();
    schema.addColumn("col1", DataType.BOOLEAN);
    schema.addColumn("col2", DataType.BYTE);
    schema.addColumn("col3", DataType.SHORT);
    schema.addColumn("col4", DataType.INT);
    schema.addColumn("col5", DataType.LONG);
    schema.addColumn("col6", DataType.FLOAT);
    schema.addColumn("col7", DataType.DOUBLE);
    schema.addColumn("col8", DataType.STRING);
    schema.addColumn("col9", DataType.BYTES);
    schema.addColumn("col10", DataType.IPv4);
    //schema.addColumn("col11", DataType.IPv6);
    
    Tuple tuple = new VTuple(10);
    tuple.put(
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),        
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    );
    
    byte [] bytes = TupleUtil.toBytes(schema, tuple);
    Tuple tuple2 = TupleUtil.toTuple(schema, bytes);
    
    assertEquals(tuple, tuple2);
  }
}
