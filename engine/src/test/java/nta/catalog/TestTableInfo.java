package nta.catalog;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;
import nta.catalog.proto.TableProtos.TableType;

import org.junit.Test;

public class TestTableInfo {
  @Test
  public final void testClone() {
    TableMeta meta1 = new TableMeta();
    meta1.setStorageType(StoreType.CSV);
    meta1.setTableType(TableType.BASETABLE);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    meta1.setStartKey(100);
    meta1.setEndKey(200);
    
    TableInfo info = (TableInfo) meta1;
    
    TableInfo info2 = (TableInfo) info.clone();
    assertEquals(info, info2);
  }

}
