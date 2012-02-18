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
	  Schema schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    info = TCatUtil.newTableMeta(schema, StoreType.CSV);

    desc = new TableDescImpl("table1", info, new Path("/nta/data"));
	}

  @Test
  public void test() throws CloneNotSupportedException {
    Schema schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    TableMeta info = TCatUtil.newTableMeta(schema, StoreType.CSV);
    testClone(info);

    TableDesc desc = new TableDescImpl("table1", info, new Path("/nta/data"));
    assertEquals("table1", desc.getId());
    
    assertEquals(new Path("/nta/data"), desc.getPath());
    assertEquals(info, desc.getMeta());
    testClone(desc);
  }
  
  @Test
  public void testTableMetaToJson() throws CloneNotSupportedException {
    TableMeta meta = new TableMetaImpl(info.getProto());
    Gson gson = GsonCreator.getInstance();
    String json = meta.toJSON();
    System.out.println(json);
    TableMeta jsonMeta = gson.fromJson(json, TableMeta.class);
    assertEquals(meta.getSchema(), jsonMeta.getSchema());
    assertEquals(meta.getStoreType(), jsonMeta.getStoreType());
    assertEquals(meta, jsonMeta);
    testClone(meta);
  }
  
  @Test
  public void testTableDescToJson() throws CloneNotSupportedException {
    Gson gson = GsonCreator.getInstance();

    TableDesc desc = new TableDescImpl("table1", info, new Path("/nta/data"));
    testClone(desc);

    String json = desc.toJSON();
    System.out.println(json);
    TableDesc fromJson = gson.fromJson(json, TableDesc.class);
    assertEquals(desc.getId(), fromJson.getId());
    assertEquals(desc.getPath(), fromJson.getPath());
    assertEquals(desc.getMeta(), fromJson.getMeta());
    testClone(fromJson);
  }
  
  @Test
  public void testFragmentToJson() throws CloneNotSupportedException {
	  TableDesc tmp = new Fragment("frag1", new Path("/"), info, 0, 10);
	  testClone(tmp);
	  Fragment frag = new Fragment((TabletProto)tmp.getProto());
	  testClone(frag);
	  String json = frag.toJSON();
	  System.out.println(json);
	  Fragment fromJson = (Fragment)GsonCreator.getInstance().fromJson(json, TableDesc.class);
	  System.out.println(fromJson.toJSON());
	  assertEquals(frag.getId(), fromJson.getId());
	  assertEquals(frag.getPath(), fromJson.getPath());
	  assertEquals(frag.getMeta(), fromJson.getMeta());
	  assertEquals(frag.getStartOffset(), fromJson.getStartOffset());
	  assertEquals(frag.getLength(), fromJson.getLength());
	  testClone(fromJson);
  }

  public void testClone(TableDesc desc) throws CloneNotSupportedException {
    TableDesc copy = (TableDesc) desc.clone();
    assertEquals(desc, copy);
  }
  
  public void testClone(TableMeta meta) throws CloneNotSupportedException {
    TableMeta copy = (TableMeta) meta.clone();
    assertEquals(meta, copy);
  }
}