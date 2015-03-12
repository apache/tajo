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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class DeletionService {
  static final Log LOG = LogFactory.getLog(DeletionService.class);

  private int debugDelay;
  private ScheduledThreadPoolExecutor sched;
  private final FileContext lfs = getLfs();

  static final FileContext getLfs() {
    try {
      return FileContext.getLocalFSFileContext();
    } catch (UnsupportedFileSystemException e) {
      throw new RuntimeException(e);
    }
  }

  public DeletionService(int defaultThreads, int debugDelay) {
    ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("DeletionService #%d").build();

    sched = new ScheduledThreadPoolExecutor(defaultThreads, tf);
    sched.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    sched.setKeepAliveTime(60L, TimeUnit.SECONDS);
    this.debugDelay = debugDelay;
  }


  /**
   * /**
   * Delete the path(s) as this user.
   *
   * @param subDir   the sub directory name
   * @param baseDirs the base directories which contains the subDir's
   */
  public void delete(Path subDir, Path... baseDirs) {
    if (debugDelay != -1) {
      sched.schedule(new FileDeletion(subDir, baseDirs), debugDelay, TimeUnit.SECONDS);
    }
  }

  public void stop() {
    sched.shutdown();
    boolean terminated = false;
    try {
      terminated = sched.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }
    if (!terminated) {
      sched.shutdownNow();
    }
  }

  private class FileDeletion implements Runnable {
    final Path subDir;
    final Path[] baseDirs;

    FileDeletion(Path subDir, Path[] baseDirs) {
      this.subDir = subDir;
      this.baseDirs = baseDirs;
    }

    @Override
    public void run() {

      if (baseDirs == null || baseDirs.length == 0) {
        LOG.debug("Worker deleting absolute path : " + subDir);
        try {
          lfs.delete(subDir, true);
        } catch (IOException e) {
          LOG.warn("Failed to delete " + subDir, e);
        }
        return;
      }
      for (Path baseDir : baseDirs) {
        Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
        LOG.debug("Worker deleting path : " + del);
        try {
          lfs.delete(del, true);
        } catch (IOException e) {
          LOG.warn("Failed to delete " + subDir, e);
        }
      }
    }
  }
}
