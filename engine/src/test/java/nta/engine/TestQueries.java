package nta.engine;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestQueries {

  @Test
  public final void testSelect() throws Exception {
    String [] names = {"table1"};
    String [][] tables = new String[1][];
    tables[0] = new String[] {"1,b,1.0", "2,c,2.0"};

    Schema[] schemas = new Schema[1];
    schemas[0] = new Schema()
        .addColumn("f1", CatalogProtos.DataType.INT)
        .addColumn("f2", CatalogProtos.DataType.STRING)
        .addColumn("f3", CatalogProtos.DataType.FLOAT);

    ResultSet res = NtaTestingUtility.run(names, schemas, tables, "select f1,f2,f3 from table1");
    res.next();
    assertEquals(1, res.getInt(0));
    assertEquals("b", res.getString(1));
    assertTrue(1.0f == res.getFloat(2));
    res.next();
    assertEquals(2, res.getInt(0));
    assertEquals("c", res.getString(1));
    assertTrue(2.0f == res.getFloat(2));
  }

  @Test
  public final void testSelectAsterik() throws Exception {
    String [] names = {"table1"};
    String [][] tables = new String[1][];
    tables[0] = new String[] {"1,b,1.0", "2,c,2.0"};

    Schema[] schemas = new Schema[1];
    schemas[0] = new Schema()
        .addColumn("f1", CatalogProtos.DataType.INT)
        .addColumn("f2", CatalogProtos.DataType.STRING)
        .addColumn("f3", CatalogProtos.DataType.FLOAT);

    ResultSet res = NtaTestingUtility.run(names, schemas, tables, "select * from table1");
    res.next();
    assertEquals(1, res.getInt(0));
    assertEquals("b", res.getString(1));
    assertTrue(1.0f == res.getFloat(2));
    res.next();
    assertEquals(2, res.getInt(0));
    assertEquals("c", res.getString(1));
    assertTrue(2.0f == res.getFloat(2));
  }
}
