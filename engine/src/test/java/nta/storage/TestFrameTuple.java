package nta.storage;

import static org.junit.Assert.*;

import nta.datum.Datum;
import nta.datum.DatumFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFrameTuple {
  private Tuple tuple1;
  private Tuple tuple2;

  @Before
  public void setUp() throws Exception {
    tuple1 = new VTuple(11);
    tuple1.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createChar('9'),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),        
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    });
    
    tuple2 = new VTuple(11);
    tuple2.put(new Datum[] {
        DatumFactory.createBool(true),
        DatumFactory.createByte((byte) 0x99),
        DatumFactory.createChar('9'),
        DatumFactory.createShort((short) 17),
        DatumFactory.createInt(59),
        DatumFactory.createLong(23l),
        DatumFactory.createFloat(77.9f),
        DatumFactory.createDouble(271.9f),        
        DatumFactory.createString("hyunsik"),
        DatumFactory.createBytes("hyunsik".getBytes()),
        DatumFactory.createIPv4("192.168.0.1")
    });
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testFrameTuple() {
    Tuple frame = new FrameTuple(tuple1, tuple2);
    assertEquals(22, frame.size());
    for (int i = 0; i < 22; i++) {
      assertTrue(frame.contains(i));
    }
    
    assertEquals(DatumFactory.createLong(23l), frame.get(5));
    assertEquals(DatumFactory.createLong(23l), frame.get(16));
    assertEquals(DatumFactory.createIPv4("192.168.0.1"), frame.get(10));
    assertEquals(DatumFactory.createIPv4("192.168.0.1"), frame.get(21));
  }
}
