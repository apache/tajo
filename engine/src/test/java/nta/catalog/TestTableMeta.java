package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;

import org.junit.Before;
import org.junit.Test;

public class TestTableMeta {
  TableMeta meta = null;
  Schema schema = null;
  
  @Before
  public void setUp() {    
    schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
  }
  
  @Test
  public void testTableMetaTableProto() {    
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    TableMeta meta1 = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMeta meta2 = new TableMetaImpl(meta1.getProto());    
    assertEquals(meta1, meta2);
  }
  
  @Test
  public final void testClone() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    TableMeta meta1 = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMetaImpl meta2 = (TableMetaImpl) meta1.clone();
    assertEquals(meta1.getSchema(), meta2.getSchema());
    assertEquals(meta1.getStoreType(), meta2.getStoreType());
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testSchema() throws CloneNotSupportedException {
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    TableMeta meta1 = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableMeta meta2 = (TableMeta) meta1.clone();
    
    assertEquals(meta1, meta2);
  }
  
  @Test
  public void testGetStorageType() {
    assertEquals(StoreType.CSV, meta.getStoreType());
  }
  
  @Test
  public void testGetSchema() {
    Schema schema2 = new Schema();
    schema2.addColumn("name", DataType.BYTE);
    schema2.addColumn("addr", DataType.STRING);
    
    assertEquals(schema, schema2);
  }
  
  @Test
  public void testSetSchema() {
    Schema schema2 = new Schema();
    schema2.addColumn("name", DataType.BYTE);
    schema2.addColumn("addr", DataType.STRING);
    schema2.addColumn("age", DataType.INT);
    
    assertNotSame(meta.getSchema(), schema2);
    meta.setSchema(schema2);
    assertEquals(meta.getSchema(), schema2);
  }
  
  @Test
  public void testEqualsObject() {   
    Schema schema2 = new Schema();
    schema2.addColumn("name", DataType.BYTE);
    schema2.addColumn("addr", DataType.STRING);
    TableMeta meta2 = TCatUtil.newTableMeta(schema2, StoreType.CSV);
    
    assertTrue(meta.equals(meta2));
    
    assertNotSame(meta, meta2);
  }
  
  @Test
  public void testGetProto() {    
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    TableMeta meta1 = TCatUtil.newTableMeta(schema1, StoreType.CSV);
    
    TableProto proto = meta1.getProto();
    TableMeta newMeta = new TableMetaImpl(proto);
    
    assertTrue(meta1.equals(newMeta));
  }   
}
