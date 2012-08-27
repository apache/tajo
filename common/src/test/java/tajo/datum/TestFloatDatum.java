package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestFloatDatum {

	@Test
	public final void testType() {
		Datum d = DatumFactory.createFloat(1f);
		assertEquals(DatumType.FLOAT, d.type());
	}

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createFloat(5f);
		assertEquals(5,d.asInt());
	}

	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createFloat(5f);
		assertEquals(5l,d.asLong());		
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.createFloat(5f);
		assertTrue(5.0f == d.asFloat());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.createFloat(5f);
		assertTrue(5.0d == d.asDouble());
	}

	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createFloat(5f);
		assertEquals("5.0", d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createFloat(5f);
    assertEquals(4, d.size());
  }
}
