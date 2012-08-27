package tajo.catalog;

import org.junit.Test;
import tajo.catalog.proto.CatalogProtos.DataType;

import static org.junit.Assert.assertEquals;

public class TestCatalogUtil {
  @Test
  public final void testGetCanonicalName() {
    String canonical = TCatUtil.getCanonicalName("sum",
        new DataType[]{DataType.INT, DataType.LONG});
    assertEquals("sum(INT,LONG)", canonical);
  }
}
