package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @author Hyunsik Choi
 * 
 */
public class TestIntDatum {

  @Test
  public final void testType() {
    Datum d = DatumFactory.createInt(1);
    assertEquals(DatumType.INT, d.type());
  }

  @Test
  public final void testAsInt() {
    Datum d = DatumFactory.createInt(5);
    assertEquals(5, d.asInt());
  }

  @Test
  public final void testAsLong() {
    Datum d = DatumFactory.createInt(5);
    assertEquals(5, d.asLong());
  }

  @Test
  public final void testAsFloat() {
    Datum d = DatumFactory.createInt(5);
    assertTrue(5.0f == d.asFloat());
  }

  @Test
  public final void testAsDouble() {
    Datum d = DatumFactory.createInt(5);
    assertTrue(5.0d == d.asDouble());
  }

  @Test
  public final void testAsChars() {
    Datum d = DatumFactory.createInt(5);
    assertEquals("5", d.asChars());
  }

  @Test
  public final void testSize() {
    Datum d = DatumFactory.createInt(5);
    assertEquals(4, d.size());
  }
}
