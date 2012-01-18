package nta.datum;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestDatumFactory {
	
	@Test
	public final void testCreateByte() {
		Datum d = DatumFactory.createByte((byte)5);
		assertEquals(d.type(),DatumType.BYTE);
	}

	@Test
	public final void testCreateShort() {
		Datum d = DatumFactory.createShort((short)5);
		assertEquals(d.type(),DatumType.SHORT);
	}
	
	@Test
	public final void testCreateInt() {
		Datum d = DatumFactory.createInt(5);
		assertEquals(d.type(),DatumType.INT);
	}
	
	@Test
	public final void testCreateLong() {
		Datum d = DatumFactory.createLong((long)5);
		assertEquals(d.type(),DatumType.LONG);
	}

	@Test
	public final void testCreateFloat() {
		Datum d = DatumFactory.createFloat(5.0f);
		assertEquals(d.type(),DatumType.FLOAT);
	}

	@Test
	public final void testCreateDouble() {
		Datum d = DatumFactory.createDouble(5.0d);
		assertEquals(d.type(),DatumType.DOUBLE);
	}

	@Test
	public final void testCreateBoolean() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(d.type(),DatumType.BOOLEAN);
	}

	@Test
	public final void testCreateString() {
		Datum d = DatumFactory.createString("12345a");
		assertEquals(d.type(),DatumType.STRING);
	}
}
