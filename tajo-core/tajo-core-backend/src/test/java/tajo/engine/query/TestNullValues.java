/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.engine.query;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import tajo.IntegrationTest;
import tajo.TajoTestingCluster;
import tajo.catalog.Options;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.storage.CSVFile;

import java.sql.ResultSet;

import static org.junit.Assert.*;

/**
 * This is the unit test for null values. This test needs specialized data sets.
 * So, We separated it from other unit tests using TPC-H data set.
 */
@Category(IntegrationTest.class)
public class TestNullValues {

  @Test
  public final void testIsNull() throws Exception {
    String [] table = new String[] {"nulltable1"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.INT);
    schema.addColumn("col2", CatalogProtos.DataType.STRING);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|",
        "2||",
        "3|filled|"
    };
    Options opts = new Options();
    opts.put(CSVFile.DELIMITER, "|");
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable1 where col2 is null");
    assertTrue(res.next());
    assertEquals(2, res.getInt(1));
    assertFalse(res.next());
  }

  @Test
  public final void testIsNotNull() throws Exception {
    String [] table = new String[] {"nulltable2"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.INT);
    schema.addColumn("col2", CatalogProtos.DataType.STRING);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|",
        "2||",
        "3|filled|"
    };
    Options opts = new Options();
    opts.put(CSVFile.DELIMITER, "|");
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable2 where col2 is not null");
    assertTrue(res.next());
    assertEquals(1, res.getInt(1));
    assertTrue(res.next());
    assertEquals(3, res.getInt(1));
    assertFalse(res.next());
  }

  @Test
  public final void testIsNotNull2() throws Exception {
    String [] table = new String[] {"nulltable3"};
    Schema schema = new Schema();
    schema.addColumn("col1", CatalogProtos.DataType.LONG);
    schema.addColumn("col2", CatalogProtos.DataType.LONG);
    schema.addColumn("col3", CatalogProtos.DataType.LONG);
    schema.addColumn("col4", CatalogProtos.DataType.LONG);
    schema.addColumn("col5", CatalogProtos.DataType.LONG);
    schema.addColumn("col6", CatalogProtos.DataType.LONG);
    schema.addColumn("col7", CatalogProtos.DataType.LONG);
    schema.addColumn("col8", CatalogProtos.DataType.LONG);
    schema.addColumn("col9", CatalogProtos.DataType.LONG);
    schema.addColumn("col10", CatalogProtos.DataType.LONG);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        ",,,,672287821,1301460,1,313895860387,126288907,1024",
        ",,,43578,19,13,6,3581,2557,1024"
    };
    Options opts = new Options();
    opts.put(CSVFile.DELIMITER, ",");
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable3 where col1 is null and col2 is null and col3 is null and col4 = 43578");
    assertTrue(res.next());
    assertEquals(43578, res.getLong(4));
    assertFalse(res.next());
  }
}
