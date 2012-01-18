package nta.catalog;

import static org.junit.Assert.assertEquals;

import java.net.URI;

import nta.catalog.proto.TableProtos.DataType;
import nta.catalog.proto.TableProtos.StoreType;

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
    desc.setURI(new Path("/nta/data"));
    assertEquals(URI.create("/nta/data"), desc.getURI());    
    assertEquals(info, desc.getMeta());
  }
}