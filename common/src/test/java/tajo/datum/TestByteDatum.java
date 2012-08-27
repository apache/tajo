package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestByteDatum {

	@Test
	public final void testType() {
		Datum d = DatumFactory.createByte((byte) 1);
		assertEquals(DatumType.BYTE, d.type());
	}

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createByte((byte)5);
		assertEquals(5,d.asInt());
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createByte((byte)5);
		assertEquals(5l,d.asLong());
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.createByte((byte)5);
		assertEquals(5,d.asLong());
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.createByte((byte)5);
		assertTrue(5.0f == d.asFloat());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.createByte((byte)5);
		assertTrue(5.0d == d.asDouble());
	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createByte((byte)5);
		System.out.println(d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createByte((byte) 1);
    assertEquals(1, d.size());
  }
}
