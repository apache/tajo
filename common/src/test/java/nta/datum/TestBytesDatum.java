package nta.datum;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
}
