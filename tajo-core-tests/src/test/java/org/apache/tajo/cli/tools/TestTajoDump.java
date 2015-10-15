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

package org.apache.tajo.cli.tools;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.storage.TablespaceManager;
import org.apache.tajo.util.FileUtil;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTajoDump extends QueryTestCaseBase {

  @Test
  public void testDump1() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE TABLE \"" + getCurrentDatabase() +
          "\".\"TableName1\" (\"Age\" int, \"FirstName\" TEXT, lastname TEXT)");

      try {
        UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(bos);
        TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
        printWriter.flush();
        printWriter.close();
        assertStrings(new String(bos.toByteArray()));
        bos.close();
      } finally {
        executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName1\"");
      }
    }
  }

  @Test
  public void testDump2() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE TABLE \"" + getCurrentDatabase() +
          "\".\"TableName2\" (\"Age\" int, \"Name\" Record (\"FirstName\" TEXT, lastname TEXT))");

      try {
        UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(bos);
        TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
        printWriter.flush();
        printWriter.close();
        assertStrings(new String(bos.toByteArray()));
        bos.close();
      } finally {
        executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName2\"");
      }
    }
  }

  @Test
  public void testDump4() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE TABLE \"" + getCurrentDatabase() +
        "\".\"TableName1\" (\"Age\" int, \"FirstName\" TEXT, lastname TEXT)");

      executeString("CREATE INDEX test_idx on \"" + getCurrentDatabase()
        + "\".\"TableName1\" ( \"Age\" asc null first, \"FirstName\" desc null last )");

      try {
        UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(bos);
        TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
        printWriter.flush();
        printWriter.close();

        assertOutputResult("testDump3.result", new String(bos.toByteArray()), new String[]{"${index.path}"},
          new String[]{TablespaceManager.getDefault().getTableUri(getCurrentDatabase(), "test_idx").toString()});
        bos.close();
      } finally {
        executeString("DROP INDEX test_idx");
        executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName1\"");
      }
    }
  }

  @Test
  public void testPartitionsDump() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("create table \"" + getCurrentDatabase() + "\".\"TableName3\""
          + " (\"col1\" int4, \"col2\" int4) "
          + " partition by column(\"col3\" int4, \"col4\" int4)"
      );

      executeString("ALTER TABLE \"" + getCurrentDatabase() + "\".\"TableName3\"" +
        " ADD PARTITION (\"col3\" = 1 , \"col4\" = 2)");

      executeString("create table \"" + getCurrentDatabase() + "\".\"TableName4\""
          + " (\"col1\" int4, \"col2\" int4) "
          + " partition by column(\"col3\" TEXT, \"col4\" date)"
      );

      executeString("ALTER TABLE \"" + getCurrentDatabase() + "\".\"TableName4\"" +
        " ADD PARTITION (\"col3\" = 'tajo' , \"col4\" = '2015-09-01')");

      try {
        UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(bos);
        TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
        printWriter.flush();
        printWriter.close();

        String[] paramValues = new String[] {
          TablespaceManager.getDefault().getTableUri(getCurrentDatabase(), "TableName3").toString()
          , TablespaceManager.getDefault().getTableUri(getCurrentDatabase(), "TableName4").toString()
        };

        assertOutputResult("testPartitionsDump.result", new String(bos.toByteArray())
          , new String[]{"${partition.path1}", "${partition.path2}"}, paramValues);
        bos.close();
      } finally {
        executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName3\"");
        executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName4\"");
      }
    }
  }

  private void assertOutputResult(String expectedResultFile, String actual, String[] paramKeys, String[] paramValues)
      throws Exception {
    FileSystem fs = currentResultPath.getFileSystem(testBase.getTestingCluster().getConfiguration());
    Path resultFile = StorageUtil.concatPath(currentResultPath, expectedResultFile);
    assertTrue(resultFile.toString() + " existence check", fs.exists(resultFile));

    String expectedResult = FileUtil.readTextFile(new File(resultFile.toUri()));

    if (paramKeys != null) {
      for (int i = 0; i < paramKeys.length; i++) {
        if (i < paramValues.length) {
          expectedResult = expectedResult.replace(paramKeys[i], paramValues[i]);
        }
      }
    }
    assertEquals(expectedResult.trim(), actual.trim());
  }
}
