package nta.datum;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestDatumFactory {
	
	@Test
	public final void testCreateByte() {
		Datum d = DatumFactory.create((byte)5);
		assertEquals(d.type(),DatumType.BYTE);
	}

	@Test
	public final void testCreateShort() {
		Datum d = DatumFactory.create((short)5);
		assertEquals(d.type(),DatumType.SHORT);
	}
	
	@Test
	public final void testCreateInt() {
		Datum d = DatumFactory.create(5);
		assertEquals(d.type(),DatumType.INT);
	}
	
	@Test
	public final void testCreateLong() {
		Datum d = DatumFactory.create((long)5);
		assertEquals(d.type(),DatumType.LONG);
	}

	@Test
	public final void testCreateFloat() {
		Datum d = DatumFactory.create(5.0f);
		assertEquals(d.type(),DatumType.FLOAT);
	}

	@Test
	public final void testCreateDouble() {
		Datum d = DatumFactory.create(5.0d);
		assertEquals(d.type(),DatumType.DOUBLE);
	}

	@Test
	public final void testCreateBoolean() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.type(),DatumType.BOOLEAN);
	}

	@Test
	public final void testCreateString() {
		Datum d = DatumFactory.create("12345a");
		assertEquals(d.type(),DatumType.STRING);
	}
}
