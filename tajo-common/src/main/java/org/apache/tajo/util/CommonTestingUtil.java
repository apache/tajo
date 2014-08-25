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

package org.apache.tajo.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.ConfigKey;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.SessionVars;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;
import java.util.UUID;

public class CommonTestingUtil {
  public static final String TAJO_TEST_KEY = "tajo.test.enabled";
  public static final String TAJO_TEST_TRUE = "true";
  private static OverridableConf userSessionVars;

  static {
    System.setProperty(CommonTestingUtil.TAJO_TEST_KEY, CommonTestingUtil.TAJO_TEST_TRUE);

    userSessionVars = new OverridableConf(new TajoConf(), ConfigKey.ConfigType.SESSION);
    for (SessionVars var : SessionVars.values()) {
      String value = System.getProperty(var.keyname());
      if (value != null) {
        userSessionVars.put(var, value);
      }
    }
  }

  public static OverridableConf getSessionVarsForTest() {
    return userSessionVars;
  }

  /**
   *
   * @param dir a local directory to be created
   * @return  the created path
   * @throws java.io.IOException
   */
  public static Path getTestDir(String dir) throws IOException {
    Path path = new Path(dir);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    cleanupTestDir(dir);
    fs.mkdirs(path);

    return fs.makeQualified(path);
  }

  public static void cleanupTestDir(String dir) throws IOException {
    Path path = new Path(dir);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(fs.exists(path)) {
      fs.delete(path, true);
    }
  }

  public static Path getTestDir() throws IOException {
    String randomStr = UUID.randomUUID().toString();
    Path path = new Path("target/test-data", randomStr);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if(fs.exists(path)) {
      fs.delete(path, true);
    }

    fs.mkdirs(path);

    return fs.makeQualified(path);
  }
}
