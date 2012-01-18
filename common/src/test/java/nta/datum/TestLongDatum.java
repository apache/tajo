package nta.datum;

import static org.junit.Assert.*;

import nta.datum.Datum;
import nta.datum.DatumFactory;
import nta.datum.DatumType;

import org.junit.Test;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TestLongDatum {

	@Test
	public final void testType() {
		Datum d = DatumFactory.createLong(1l);
		assertEquals(d.type(),DatumType.LONG);
	}

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createLong(5l);
		assertEquals(5,d.asInt());
	}

	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createLong(5l);
		assertEquals(5l,d.asLong());		
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.createLong(5l);
		assertTrue(5.0f == d.asFloat());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.createLong(5l);
		assertTrue(5.0d == d.asDouble());
	}

	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createLong(5l);
		assertEquals("5", d.asChars());
	}
}
