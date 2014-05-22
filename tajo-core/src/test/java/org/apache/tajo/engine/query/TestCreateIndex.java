/*
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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class TestCreateIndex extends QueryTestCaseBase {

  public TestCreateIndex() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  private static void assertIndexExist(String indexName) throws IOException {
    Path indexPath = new Path(conf.getVar(ConfVars.INDEX_DIR), indexName);
    System.out.println(indexPath);
    FileSystem fs = indexPath.getFileSystem(conf);
    assertTrue(fs.exists(indexPath));
    assertEquals(2, fs.listStatus(indexPath).length);
    fs.deleteOnExit(indexPath);
  }

  @Test
  public final void testCreateIndex() throws Exception {
    executeQuery();
    assertIndexExist("l_orderkey_idx");
  }

  @Test
  public final void testCreateIndexOnMultiAttrs() throws Exception {
    executeQuery();
    assertIndexExist("l_orderkey_partkey_idx");
  }

  @Test
  public final void testCreateIndexWithCondition() throws Exception {
    executeQuery();
    assertIndexExist("l_orderkey_partkey_lt10_idx");
  }

  @Test
  public final void testCreateIndexOnExpression() throws Exception {
    executeQuery();
    assertIndexExist("l_orderkey_100_lt10_idx");
  }
}
