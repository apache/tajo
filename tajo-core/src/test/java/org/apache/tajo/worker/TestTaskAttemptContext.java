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

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTaskAttemptContext {

  @Test
  public void testWorkerPath() throws Exception {
    TajoConf conf = new TajoConf();
    List<String> tempDirs = Lists.newArrayList();

    for (int i = 0; i < 10; i++){
      tempDirs.add(CommonTestingUtil.getTestDir().toUri().getPath());
    }
    conf.setVar(TajoConf.ConfVars.WORKER_TEMPORAL_DIR, StringUtils.join(tempDirs, ","));

    Path testPath = new Path("testData");
    TaskAttemptContext context = new TaskAttemptContext(conf, new QueryContext(),
        LocalTajoTestingUtility.newQueryUnitAttemptId(),
        new Fragment[0], testPath);

    assertEquals(testPath, context.getWorkPath());
    Path workPath = context.getWorkDir();
    int notSame = 0;
    for (int i = 0; i < 10; i++){
       if(!workPath.equals(context.getWorkDir())){
          notSame++;
       }
    }
    assertTrue(notSame > 0);
  }
}
