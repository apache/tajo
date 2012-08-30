package tajo.catalog;

import org.apache.hadoop.fs.Path;
import org.junit.Test;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.TajoTestingUtility;

import static org.junit.Assert.assertEquals;

/**
 * @author Hyunsik Choi
 */
public class TestCatalogStore {
  
  @Test
  public final void test() throws Exception {
    TajoTestingUtility util = new TajoTestingUtility();
    util.startMiniZKCluster();
    util.getConfiguration().set(TConstants.STORE_CLASS,
        "tajo.catalog.store.DBStore");
    util.startCatalogCluster();
    
    CatalogService catalog = util.getMiniCatalogCluster().getCatalog();
    
    Schema schema = new Schema();
    schema.addColumn("id", DataType.INT)
    .addColumn("name", DataType.STRING)
    .addColumn("age", DataType.INT)
    .addColumn("score", DataType.DOUBLE);
    
    int numTables = 5;
    for (int i = 0; i < numTables; i++) {
      String tableName = "tableA_" + i;
      TableMeta meta = TCatUtil.newTableMeta(schema, StoreType.CSV);
      TableDesc desc = TCatUtil.newTableDesc(tableName, meta,
          new Path("/tableA_" + i));
      catalog.addTable(desc);
    }
    
    assertEquals(numTables, catalog.getAllTableNames().size());    
    util.shutdownCatalogCluster();
    
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    assertEquals(numTables, catalog.getAllTableNames().size());
    
    util.shutdownCatalogCluster();
    util.shutdownMiniZKCluster();    
  }
}
