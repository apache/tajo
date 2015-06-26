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

package org.apache.tajo.storage.hbase;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestColumnMapping {
  @Test
  public void testColumnKeyValueMapping() throws Exception {
    KeyValueSet keyValueSet = new KeyValueSet();
    keyValueSet.set(HBaseStorageConstants.META_TABLE_KEY, "test");
    keyValueSet.set(HBaseStorageConstants.META_COLUMNS_KEY, ":key,col2:key:,col2:value:#b,col3:");

    Schema schema = new Schema();
    schema.addColumn("c1", Type.TEXT);
    schema.addColumn("c2", Type.TEXT);
    schema.addColumn("c3", Type.TEXT);
    schema.addColumn("c4", Type.TEXT);

    TableMeta tableMeta = new TableMeta("HBASE", keyValueSet);

    ColumnMapping columnMapping = new ColumnMapping(schema, tableMeta.getOptions());

    List<String> cfNames = columnMapping.getColumnFamilyNames();
    assertEquals(2, cfNames.size());
    assertEquals("col2", cfNames.get(0));
    assertEquals("col3", cfNames.get(1));

    for (int i = 0; i < columnMapping.getIsBinaryColumns().length; i++) {
      if (i == 2) {
        assertTrue(columnMapping.getIsBinaryColumns()[i]);
      } else {
        assertFalse(columnMapping.getIsBinaryColumns()[i]);
      }
    }

    for (int i = 0; i < columnMapping.getIsRowKeyMappings().length; i++) {
      if (i == 0) {
        assertTrue(columnMapping.getIsRowKeyMappings()[i]);
      } else {
        assertFalse(columnMapping.getIsRowKeyMappings()[i]);
      }
    }

    String[] expectedColumnNames = { null, null, null, null};
    for (int i = 0; i < schema.size(); i++) {
      String columnName = columnMapping.getMappingColumns()[i][1] == null ? null :
          new String(columnMapping.getMappingColumns()[i][1]);
      assertEquals(expectedColumnNames[i], columnName);
    }

    for (int i = 0; i < schema.size(); i++) {
      if (i == 1) {
        assertTrue(columnMapping.getIsColumnKeys()[i]);
      } else {
        assertFalse(columnMapping.getIsColumnKeys()[i]);
      }
    }

    for (int i = 0; i < schema.size(); i++) {
      if (i == 2) {
        assertTrue(columnMapping.getIsColumnValues()[i]);
      } else {
        assertFalse(columnMapping.getIsColumnValues()[i]);
      }
    }
  }
}
