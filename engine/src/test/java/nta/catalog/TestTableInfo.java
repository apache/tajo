package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.StoreType;
import nta.catalog.proto.CatalogProtos.TableProto;

import org.junit.Before;
import org.junit.Test;

public class TestTableInfo {
  TableMeta meta = null;
  Schema schema = null;
  
  @Before
  public void setUp() {
    meta = new TableMetaImpl();
    meta.setStorageType(StoreType.CSV);
    schema = new Schema();
    schema.addColumn("name", DataType.BYTE);
    schema.addColumn("addr", DataType.STRING);
    meta.setSchema(schema);
  }
  
  @Test
  public void testTableMetaTableProto() {
    TableMeta meta1 = new TableMetaImpl();
    meta1.setStorageType(StoreType.CSV);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    
    TableMeta meta2 = new TableMetaImpl(meta1.getProto());    
    assertEquals(meta1, meta2);
  }
  
  @Test
  public final void testClone() throws CloneNotSupportedException {
    TableMetaImpl meta1 = new TableMetaImpl();
    meta1.setStorageType(StoreType.CSV);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    
    TableMetaImpl info = (TableMetaImpl) meta1;
    
    TableMetaImpl info2 = (TableMetaImpl) info.clone();
    assertEquals(info.getSchema(), info2.getSchema());
    assertEquals(info.getStoreType(), info2.getStoreType());
    assertEquals(info.getOptions(), info2.getOptions());
    assertEquals(info, info2);
  }
  
  @Test
  public void testSchema() throws CloneNotSupportedException {
    TableMeta meta1 = new TableMetaImpl();
    meta1.setStorageType(StoreType.CSV);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    
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
    TableMeta meta2 = new TableMetaImpl();
    meta2.setStorageType(StoreType.CSV);
    Schema schema2 = new Schema();
    schema2.addColumn("name", DataType.BYTE);
    schema2.addColumn("addr", DataType.STRING);
    meta2.setSchema(schema2);
    
    assertTrue(meta.equals(meta2));
    
    assertNotSame(meta, meta2);
  }
  
  @Test
  public void testGetProto() {
    TableMeta meta1 = new TableMetaImpl();
    meta1.setStorageType(StoreType.CSV);
    Schema schema1 = new Schema();
    schema1.addColumn("name", DataType.BYTE);
    schema1.addColumn("addr", DataType.STRING);
    meta1.setSchema(schema1);
    
    TableProto proto = meta1.getProto();
    TableMeta newMeta = new TableMetaImpl(proto);
    
    assertTrue(meta1.equals(newMeta));
  }   
}
