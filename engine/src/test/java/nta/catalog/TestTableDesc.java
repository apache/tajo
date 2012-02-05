package nta.catalog;

import static org.junit.Assert.assertEquals;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TabletProto;
import nta.engine.ipc.protocolrecords.Fragment;
import nta.engine.json.GsonCreator;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

public class TestTableDesc {
	TableMeta info;
	TableDesc desc;
	
	@Before
	public void setup() {
		info = new TableMetaImpl();  
	    info.setStorageType(StoreType.CSV);
	    Schema schema = new Schema();
	    schema.addColumn("name", DataType.BYTE);
	    schema.addColumn("addr", DataType.STRING);
	    info.setSchema(schema);
	    
	    desc = new TableDescImpl("table1", info);
	    desc.setPath(new Path("/nta/data"));
	}

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
  
  @Test
  public void testTableMetaToJson() {
	  TableMeta meta = new TableMetaImpl(info.getProto());
	  Gson gson = GsonCreator.getInstance();
	    String json = meta.toJSON();
	    System.out.println(json);
	    TableMeta jsonMeta = gson.fromJson(json, TableMeta.class);
	    assertEquals(meta.getSchema(), jsonMeta.getSchema());
	    assertEquals(meta.getStoreType(), jsonMeta.getStoreType());
	    assertEquals(meta.getOptions(), jsonMeta.getOptions());
  }
  
  @Test
  public void testTableDescToJson() {
	  Gson gson = GsonCreator.getInstance();

	    TableDesc desc = new TableDescImpl("table1", info);
	    desc.setPath(new Path("/nta/data"));
	    
	    String json = desc.toJSON();
	    System.out.println(json);
	    TableDesc fromJson = gson.fromJson(json, TableDesc.class);
	    assertEquals(desc.getId(), fromJson.getId());
	    assertEquals(desc.getPath(), fromJson.getPath());
	    assertEquals(desc.getMeta(), fromJson.getMeta());
  }
  
  @Test
  public void testFragmentToJson() {
	  TableDesc tmp = new Fragment("frag1", new Path("/"), info, 0, 10);
	  Fragment frag = new Fragment((TabletProto)tmp.getProto());
	  String json = frag.toJSON();
	  System.out.println(json);
	  Fragment fromJson = (Fragment)GsonCreator.getInstance().fromJson(json, TableDesc.class);
	  System.out.println(fromJson.toJSON());
	  assertEquals(frag.getId(), fromJson.getId());
	  assertEquals(frag.getPath(), fromJson.getPath());
	  assertEquals(frag.getMeta(), fromJson.getMeta());
	  assertEquals(frag.getStartOffset(), fromJson.getStartOffset());
	  assertEquals(frag.getLength(), fromJson.getLength());
  }
}