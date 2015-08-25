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
import org.apache.tajo.QueryTestCaseBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

@Category(IntegrationTest.class)
public class TestDropTable extends QueryTestCaseBase {

  @Test
  public final void testDropManagedTable() throws Exception {
    List<String> createdNames = executeDDL("table1_ddl.sql", "table1.tbl", "abc");
    assertTableExists(createdNames.get(0));
    executeDDL("drop_table_ddl.sql", null);
    assertTableNotExists("abc");
  }
}
