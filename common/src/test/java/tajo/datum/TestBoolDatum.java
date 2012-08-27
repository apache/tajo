package tajo.datum;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestBoolDatum {
	
	@Test
	public final void testType() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(DatumType.BOOLEAN, d.type());
	}
	
	@Test
	public final void testAsBool() {
		Datum d = DatumFactory.createBool(false);
		assertEquals(false, d.asBool());
	}
	
	@Test
	public final void testAsShort() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(1, d.asShort());
	}
	
	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(1, d.asInt());
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.createBool(false);
		assertEquals(0, d.asLong());
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.createBool(true);
		assertEquals(0x01, d.asByte());
	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.createBool(true);
		assertEquals("true", d.asChars());
	}
	
	@Test
  public final void testSize() {
    Datum d = DatumFactory.createBool(true);
    assertEquals(1, d.size());
  }
}