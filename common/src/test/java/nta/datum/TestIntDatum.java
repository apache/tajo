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
public class TestIntDatum {

	@Test
	public final void testType() {
		Datum d = DatumFactory.create(1);
		assertEquals(d.type(),DatumType.INT);
	}

	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.create(5);
		assertEquals(5,d.asInt());
	}

	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.create(5);
		assertEquals(5,d.asLong());
	}

	@Test
	public final void testAsFloat() {
		Datum d = DatumFactory.create(5);
		assertTrue(5.0f == d.asFloat());
	}

	@Test
	public final void testAsDouble() {
		Datum d = DatumFactory.create(5);
		assertTrue(5.0d == d.asDouble());
	}

	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.create(5);
		assertEquals("5", d.asChars());
	}
}
