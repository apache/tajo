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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import tajo.QueryUnitAttemptId;
import tajo.conf.TajoConf;
import tajo.engine.MasterWorkerProtos.Command;
import tajo.engine.MasterWorkerProtos.PingResponseProto;
import tajo.engine.MasterWorkerProtos.QueryStatus;

import java.io.IOException;

public class MockupNormalWorker extends MockupWorker {
  private final static Log LOG = LogFactory.getLog(MockupNormalWorker.class);

  public MockupNormalWorker(TajoConf conf) {
    super(conf, Type.NORMAL);
  }

  @Override
  public void run() {
    try {
      prepareServing();
      participateCluster();
      if (!this.stopped) {
        long before = -1;
        long sleeptime = 3000;
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

          PingResponseProto response = sendHeartbeat(time);
          before = time;

          QueryUnitAttemptId qid;
          MockupTask task;
          QueryStatus status;
          for (Command cmd : response.getCommandList()) {
            qid = new QueryUnitAttemptId(cmd.getId());
            if (!taskMap.containsKey(qid)) {
              LOG.error("ERROR: no such task " + qid);
              continue;
            }
            task = taskMap.get(qid);
            status = task.getStatus();

            switch (cmd.getType()) {
              case FINALIZE:
                if (status == QueryStatus.QUERY_FINISHED
                    || status == QueryStatus.QUERY_DATASERVER
                    || status == QueryStatus.QUERY_ABORTED
                    || status == QueryStatus.QUERY_KILLED) {
                  task.setStatus(QueryStatus.QUERY_FINISHED);
                } else {
                  task.setStatus(QueryStatus.QUERY_ABORTED);
                }

                break;
              case STOP:
                if (status == QueryStatus.QUERY_INPROGRESS) {
                  task.setStatus(QueryStatus.QUERY_ABORTED);
                } else {
                  LOG.error("ERROR: Illegal State of " + qid + "(" + status + ")");
                }

                break;
            }
          }
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (KeeperException e) {
      e.printStackTrace();
    } finally {
      clear();
    }
  }
}
