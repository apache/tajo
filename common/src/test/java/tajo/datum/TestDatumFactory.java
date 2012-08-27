package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
public class TestDatumFactory {
	
	@Test
	public final void testCreateByte() {
		Datum d = DatumFactory.createByte((byte) 5);
		assertEquals(DatumType.BYTE, d.type());
	}

	@Test
	public final void testCreateShort() {
		Datum d = DatumFactory.createShort((short)5);
		assertEquals(DatumType.SHORT, d.type());
	}
	
	@Test
	public final void testCreateInt() {
		Datum d = DatumFactory.createInt(5);
		assertEquals(DatumType.INT, d.type());
	}
	
	@Test
	public final void testCreateLong() {
		Datum d = DatumFactory.createLong((long)5);
		assertEquals(DatumType.LONG, d.type());
	}

	@Test
	public final void testCreateFloat() {
		Datum d = DatumFactory.createFloat(5.0f);
		assertEquals(DatumType.FLOAT, d.type());
	}

	@Test
	public final void testCreateDouble() {
		Datum d = DatumFactory.createDouble(5.0d);
		assertEquals(DatumType.DOUBLE, d.type());
	}

	@Test
	public final void testCreateBoolean() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(DatumType.BOOLEAN, d.type());
	}

	@Test
	public final void testCreateString() {
		Datum d = DatumFactory.createString("12345a");
		assertEquals(DatumType.STRING, d.type());
	}
}
