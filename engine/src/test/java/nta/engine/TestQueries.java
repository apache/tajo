package nta.engine;

import nta.catalog.Schema;
import nta.catalog.proto.CatalogProtos;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Hyunsik Choi
 */
public class TestQueries {
  String [] names;
  String [][] tables;
  Schema[] schemas;

  public TestQueries() {
    names = new String[] {"table1"};
    tables = new String[1][];
    tables[0] = new String[] {"1,a,1.0", "2,b,2.0", "3,,3.0"};

    schemas = new Schema[1];
    schemas[0] = new Schema()
        .addColumn("f1", CatalogProtos.DataType.INT)
        .addColumn("f2", CatalogProtos.DataType.STRING)
        .addColumn("f3", CatalogProtos.DataType.FLOAT);
  }

  @Test
  public final void testSelect() throws Exception {
    ResultSet res = NtaTestingUtility.run(names, schemas, tables, "select f1,f2,f3 from table1");
    assertSelect(res);
  }

  @Test
  public final void testSelectAsterik() throws Exception {
    ResultSet res = NtaTestingUtility.run(names, schemas, tables, "select * from table1");
    assertSelect(res);
  }

  public void assertSelect(ResultSet res) throws SQLException {
    res.next();
    assertEquals(1, res.getInt(1));
    assertEquals("a", res.getString(2));
    assertTrue(1.0f == res.getFloat(3));

    res.next();
    assertEquals(2, res.getInt(1));
    assertEquals("b", res.getString(2));
    assertTrue(2.0f == res.getFloat(3));

    res.next();
    assertEquals(3, res.getInt(1));
    res.getString(2);
    res.wasNull();
    assertTrue(3.0f == res.getFloat(3));
  }
}
