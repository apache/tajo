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
import org.apache.tajo.schema.IdentifierUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

@Category(IntegrationTest.class)
public class TestCreateDatabase extends QueryTestCaseBase {

  @Test
  public final void testCreateAndDropDatabase() throws Exception {
    String databaseName = IdentifierUtil.normalizeIdentifier("testCreateAndDropDatabase");

    ResultSet res = null;
    try {
      res = executeString("CREATE DATABASE testCreateAndDropDatabase;");
      assertDatabaseExists(databaseName);
      executeString("DROP DATABASE testCreateAndDropDatabase;");
      assertDatabaseNotExists(databaseName);
    } finally {
      cleanupQuery(res);
    }
  }

  @Test
  public final void testCreateIfNotExists() throws Exception {
    String databaseName = IdentifierUtil.normalizeIdentifier("testCreateIfNotExists");

    assertDatabaseNotExists(databaseName);
    executeString("CREATE DATABASE " + databaseName + ";").close();
    assertDatabaseExists(databaseName);

    executeString("CREATE DATABASE IF NOT EXISTS " + databaseName + ";").close();
    assertDatabaseExists(databaseName);

    executeString("DROP DATABASE " + databaseName + ";").close();
    assertDatabaseNotExists(databaseName);
  }

  @Test
  public final void testDropIfExists() throws Exception {
    String databaseName = IdentifierUtil.normalizeIdentifier("testDropIfExists");
    assertDatabaseNotExists(databaseName);
    executeString("CREATE DATABASE " + databaseName + ";").close();
    assertDatabaseExists(databaseName);

    executeString("DROP DATABASE " + databaseName + ";").close();
    assertDatabaseNotExists(databaseName);

    executeString("DROP DATABASE IF EXISTS " + databaseName + ";");
    assertDatabaseNotExists(databaseName);
  }
}
