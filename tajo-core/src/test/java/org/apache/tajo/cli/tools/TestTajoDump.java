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

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.auth.UserRoleInfo;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

public class TestTajoDump extends QueryTestCaseBase {

  @Test
  public void testDump1() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE TABLE \"" + getCurrentDatabase() +
          "\".\"TableName1\" (\"Age\" int, \"FirstName\" TEXT, lastname TEXT)");

      UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      PrintWriter printWriter = new PrintWriter(bos);
      TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
      printWriter.flush();
      printWriter.close();
      assertStrings(new String(bos.toByteArray()));
      bos.close();

      executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName1\"");
    }
  }

  @Test
  public void testDump2() throws Exception {
    if (!testingCluster.isHiveCatalogStoreRunning()) {
      executeString("CREATE TABLE \"" + getCurrentDatabase() +
          "\".\"TableName2\" (\"Age\" int, \"Name\" Record (\"FirstName\" TEXT, lastname TEXT))");

      UserRoleInfo userInfo = UserRoleInfo.getCurrentUser();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      PrintWriter printWriter = new PrintWriter(bos);
      TajoDump.dump(client, userInfo, getCurrentDatabase(), false, false, false, printWriter);
      printWriter.flush();
      printWriter.close();
      assertStrings(new String(bos.toByteArray()));
      bos.close();

      executeString("DROP TABLE \"" + getCurrentDatabase() + "\".\"TableName2\"");
    }
  }
}
