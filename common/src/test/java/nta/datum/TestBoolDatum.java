package nta.datum;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestBoolDatum {
	
	@Test
	public final void testType() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.type(), DatumType.BOOLEAN);	
	}
	
	@Test
	public final void testAsBool() {
		Datum d = DatumFactory.create(false);
		assertEquals(d.asBool(), false);
	}
	
	@Test
	public final void testAsShort() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.asShort(), 1);
	}
	
	@Test
	public final void testAsInt() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.asInt(), 1);
	}
	
	@Test
	public final void testAsLong() {
		Datum d = DatumFactory.create(false);
		assertEquals(d.asLong(), 0);
	}
	
	@Test
	public final void testAsByte() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.asByte(), 0x01);
	}
	
//	@Test
//	public final void testAsFloat() {
//		Datum d = DatumFactory.create(true);
//		assertEquals(d.asFloat(), 1);
//	}
//	
//	@Test
//	public final void testAsDouble() {
//		Datum d = DatumFactory.create(true);
//		assertEquals(d.asDouble(), 1);
//	}
	
	@Test
	public final void testAsChars() {
		Datum d = DatumFactory.create(true);
		assertEquals(d.asChars(), "true");
	}
}