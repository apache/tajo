package nta.catalog;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestTableDesc {

  @Test
  public void test() {
    TableMeta info = new TableMetaImpl();  
    info.setStorageType(StoreType.CSV);
    Schema schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    info.setSchema(schema);

    TableDesc desc = new TableDescImpl("table1", info);
    assertEquals("table1", desc.getId());
    desc.setPath(new Path("/nta/data"));
    
    assertEquals(new Path("/nta/data"), desc.getPath());
    assertEquals(info, desc.getMeta());
  }
}