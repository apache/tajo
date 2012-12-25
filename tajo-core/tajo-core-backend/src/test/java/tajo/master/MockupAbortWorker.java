/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import tajo.TajoProtos.TaskAttemptState;
import tajo.conf.TajoConf;

public class MockupAbortWorker extends MockupWorker {
  public MockupAbortWorker(TajoConf conf) {
    super(conf, Type.ABORT);
  }

  private void abortTask() {
    if (taskQueue.size() > 0) {
      MockupTask task = taskQueue.remove(0);
      task.setState(TaskAttemptState.TA_FAILED);
    }
  }

  @Override
  public void run() {
    try {
      prepareServing();
      participateCluster();
      if (!this.stopped) {
        long before = -1;
        long sleeptime;
        long time;
        while (!this.stopped) {
          time = System.currentTimeMillis();
          if (before == -1) {
            sleeptime = 3000;
          } else {
            sleeptime = 3000 - (time - before);
            if (sleeptime > 0) {
              sleep(sleeptime);
            }
          }
          progressTask();
          abortTask();

          sendHeartbeat(time);
          before = time;
        }
      }
    } catch (Exception e) {
    } finally {
      clear();
    }
  }
}
