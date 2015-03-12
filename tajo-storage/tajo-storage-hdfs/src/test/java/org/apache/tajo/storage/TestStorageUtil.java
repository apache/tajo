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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TestStorageUtil {
  private TajoConf conf;
  private static String TEST_PATH = "target/test-data/TestStorageUtil";
  private Path testDir;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    conf = new TajoConf();
    testDir = CommonTestingUtil.getTestDir(TEST_PATH);
    fs = testDir.getFileSystem(conf);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public final void testGetMaxFileSequence() throws IOException {
    for (int i = 0; i < 7; i++) {
      fs.create(new Path(testDir, "part-00-00000-00"+i), false);
    }

    assertEquals(6, StorageUtil.getMaxFileSequence(fs, testDir, true));
  }
}
