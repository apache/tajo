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

package org.apache.tajo.worker;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDeletionService {
  DeletionService deletionService;

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() {
    if(deletionService != null){
      deletionService.stop();
    }
  }

  @Test
  public final void testTemporalDirectory() throws IOException, InterruptedException {
    int delay = 1;
    deletionService = new DeletionService(1, delay);
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Path tempPath = CommonTestingUtil.getTestDir();
    assertTrue(fs.exists(tempPath));
    deletionService.delete(tempPath);
    assertTrue(fs.exists(tempPath));

    Thread.sleep(delay * 2 * 1000);
    assertFalse(fs.exists(tempPath));
  }
}
