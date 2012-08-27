package tajo.datum;

import org.junit.Test;
import tajo.datum.json.GsonCreator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBytesDatum {

  @Test
  public final void testType() {
    Datum d = DatumFactory.createBytes("12345".getBytes());
    assertEquals(DatumType.BYTES, d.type());
  }
  
  @Test
  public final void testAsChars() {
    Datum d = DatumFactory.createBytes("12345".getBytes());
    assertEquals("12345", d.asChars());
  }
  
  @Test
  public final void testSize() {
    Datum d = DatumFactory.createBytes("12345".getBytes());
    assertEquals(5, d.size());
  }
  
  @Test
  public final void testJson() {
	  Datum d = DatumFactory.createBytes("12345".getBytes());
	  String json = d.toJSON();
	  Datum fromJson = GsonCreator.getInstance().fromJson(json, Datum.class);
	  assertTrue(d.equalsTo(fromJson).asBool());
  }
}
