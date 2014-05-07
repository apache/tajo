/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.StorageConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.FLOAT4);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|0.1",
        "2||",
        "3|filled|0.2"
    };
    KeyValueSet opts = new KeyValueSet();
    opts.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable1 where col3 is null");
    try {
      assertTrue(res.next());
      assertEquals(2, res.getInt(1));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testIsNotNull() throws Exception {
    String [] table = new String[] {"nulltable2"};
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT4);
    schema.addColumn("col2", Type.TEXT);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "1|filled|",
        "||",
        "3|filled|"
    };
    KeyValueSet opts = new KeyValueSet();
    opts.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable2 where col1 is not null");
    try {
      assertTrue(res.next());
      assertEquals(1, res.getInt(1));
      assertTrue(res.next());
      assertEquals(3, res.getInt(1));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testIsNotNull2() throws Exception {
    String [] table = new String[] {"nulltable3"};
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT8);
    schema.addColumn("col2", Type.INT8);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.INT8);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.INT8);
    schema.addColumn("col8", Type.INT8);
    schema.addColumn("col9", Type.INT8);
    schema.addColumn("col10", Type.INT8);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        ",,,,672287821,1301460,1,313895860387,126288907,1024",
        ",,,43578,19,13,6,3581,2557,1024"
    };
    KeyValueSet opts = new KeyValueSet();
    opts.put(StorageConstants.CSVFILE_DELIMITER, ",");
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable3 where col1 is null and col2 is null and col3 is null and col4 = 43578");
    try {
      assertTrue(res.next());
      assertEquals(43578, res.getLong(4));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }

  @Test
  public final void testIsNotNull3() throws Exception {
    String [] table = new String[] {"nulltable4"};
    Schema schema = new Schema();
    schema.addColumn("col1", Type.INT8);
    schema.addColumn("col2", Type.INT8);
    schema.addColumn("col3", Type.INT8);
    schema.addColumn("col4", Type.INT8);
    schema.addColumn("col5", Type.INT8);
    schema.addColumn("col6", Type.INT8);
    schema.addColumn("col7", Type.INT8);
    schema.addColumn("col8", Type.INT8);
    schema.addColumn("col9", Type.INT8);
    schema.addColumn("col10", Type.INT8);
    Schema [] schemas = new Schema[] {schema};
    String [] data = {
        "\\N,,,,672287821,",
        ",\\N,,43578"
    };
    KeyValueSet opts = new KeyValueSet();
    opts.put(StorageConstants.CSVFILE_DELIMITER, ",");
    opts.put(StorageConstants.CSVFILE_NULL, "\\\\N");
    ResultSet res = TajoTestingCluster
        .run(table, schemas, opts, new String[][]{data},
            "select * from nulltable4 where col1 is null and col2 is null and col3 is null and col5 is null and col4 = 43578");
    try {
      assertTrue(res.next());
      assertEquals(43578, res.getLong(4));
      assertFalse(res.next());
    } finally {
      res.close();
    }
  }
}
