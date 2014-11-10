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

package org.apache.tajo.util.history;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;

public class HistoryCleaner extends Thread {
  private static final Log LOG = LogFactory.getLog(HistoryCleaner.class);

  private int historyExpireDays;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  private Path historyParentPath;
  private Path taskHistoryParentPath;
  private TajoConf tajoConf;
  private boolean isMaster;

  public HistoryCleaner(TajoConf tajoConf, boolean isMaster) throws IOException {
    super(HistoryCleaner.class.getName());

    this.tajoConf = tajoConf;
    this.isMaster = isMaster;
    historyExpireDays = tajoConf.getIntVar(ConfVars.HISTORY_EXPIRY_TIME_DAY);
    historyParentPath = tajoConf.getQueryHistoryDir(tajoConf);
    taskHistoryParentPath = tajoConf.getTaskHistoryDir(tajoConf);
  }

  public void doStop() {
    stopped.set(true);
    this.interrupt();
  }

  @Override
  public void run() {
    LOG.info("History cleaner started: expiry day=" + historyExpireDays);
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

    while (!stopped.get()) {
      try {
        Thread.sleep(60 * 60 * 12 * 1000);   // 12 hours
      } catch (InterruptedException e) {
      }

      if (stopped.get()) {
        break;
      }

      try {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_MONTH, -historyExpireDays);

        long cleanTargetTime = cal.getTime().getTime();

        // Clean query history directory
        if (isMaster) {
          FileSystem fs = historyParentPath.getFileSystem(tajoConf);
          if (fs.exists(historyParentPath)) {
            FileStatus[] files = fs.listStatus(historyParentPath);
            if (files != null) {
              for (FileStatus eachFile : files) {
                String pathName = eachFile.getPath().getName();
                long pathTime;
                try {
                  pathTime = df.parse(pathName).getTime();
                } catch (ParseException e) {
                  LOG.warn(eachFile.getPath() + " is not History directory format.");
                  continue;
                }

                if (pathTime < cleanTargetTime) {
                  LOG.info("Cleaning query history dir: " + eachFile.getPath());
                  fs.delete(eachFile.getPath(), true);
                }
              }
            }
          }
        }

        if (!isMaster) {
          // Clean task history directory
          FileSystem fs = taskHistoryParentPath.getFileSystem(tajoConf);
          if (fs.exists(taskHistoryParentPath)) {
            FileStatus[] files = fs.listStatus(taskHistoryParentPath);
            if (files != null) {
              for (FileStatus eachFile : files) {
                String pathName = eachFile.getPath().getName();
                long pathTime;
                try {
                  pathTime = df.parse(pathName).getTime();
                } catch (ParseException e) {
                  LOG.warn(eachFile.getPath() + " is not History directory format.");
                  continue;
                }

                if (pathTime < cleanTargetTime) {
                  LOG.info("Cleaning task history dir: " + eachFile.getPath());
                  fs.delete(eachFile.getPath(), true);
                }
              }
            }
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }
    LOG.info("History cleaner stopped");
  }
}
