package nta.catalog;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.CatalogProtos.DataType;

import org.junit.Test;

public class TestCatalogUtil {
  @Test
  public final void testGetCanonicalName() {
    String canonical = TCatUtil.getCanonicalName("sum", 
        new DataType [] {DataType.INT, DataType.LONG});
    assertEquals("sum(INT,LONG)", canonical);
  }
}
