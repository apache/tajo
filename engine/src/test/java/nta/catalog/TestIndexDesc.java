package nta.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.catalog.proto.CatalogProtos.IndexDescProto;
import nta.catalog.proto.CatalogProtos.IndexType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestIndexDesc {
  static IndexDesc desc1;
  static IndexDesc desc2;
  static IndexDesc desc3;
  
  static {
    desc1 = new IndexDesc(
        "idx_test", "indexed", new Column("id", DataType.INT), 
        IndexType.TWO_LEVEL_BIN_TREE, true, true, true);
    
    desc2 = new IndexDesc(
        "idx_test2", "indexed", new Column("score", DataType.DOUBLE), 
        IndexType.TWO_LEVEL_BIN_TREE, false, false, false);
    
    desc3 = new IndexDesc(
        "idx_test", "indexed", new Column("id", DataType.INT), 
        IndexType.TWO_LEVEL_BIN_TREE, true, true, true);
  }

  @BeforeClass
  public static void setUp() throws Exception {
  }

  @AfterClass
  public static void tearDown() throws Exception {
  }

  @Test
  public void testIndexDescProto() {
    IndexDescProto proto = desc1.getProto();
    assertEquals(desc1.getProto(), proto);
    assertEquals(desc1, new IndexDesc(proto));
  }

  @Test
  public void testGetFields() {
    assertEquals("idx_test", desc1.getName());
    assertEquals("indexed", desc1.getTableId());
    assertEquals(new Column("id", DataType.INT), desc1.getColumn());
    assertEquals(IndexType.TWO_LEVEL_BIN_TREE, desc1.getIndexType());
    assertEquals(true, desc1.isUnique());
    assertEquals(true, desc1.isClustered());
    assertEquals(true, desc1.isAscending());
    
    assertEquals("idx_test2", desc2.getName());
    assertEquals("indexed", desc2.getTableId());
    assertEquals(new Column("score", DataType.DOUBLE), desc2.getColumn());
    assertEquals(IndexType.TWO_LEVEL_BIN_TREE, desc2.getIndexType());
    assertEquals(false, desc2.isUnique());
    assertEquals(false, desc2.isClustered());
    assertEquals(false, desc2.isAscending());
  }

  @Test
  public void testEqualsObject() {
    assertNotSame(desc1, desc2);
    assertEquals(desc1, desc3);
  }

  @Test
  public void testClone() throws CloneNotSupportedException {
    IndexDesc copy = (IndexDesc) desc1.clone();
    assertEquals(desc1, copy);
    assertEquals(desc3, copy);
  }
}
