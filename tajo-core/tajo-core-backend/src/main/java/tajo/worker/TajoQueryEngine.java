/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.TaskAttemptContext;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.planner.PhysicalPlanner;
import tajo.engine.planner.PhysicalPlannerImpl;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.physical.PhysicalExec;
import tajo.exception.InternalException;
import tajo.storage.StorageManager;

import java.io.IOException;

/**
 * @author Hyunsik Choi
 */
public class TajoQueryEngine {
  private final static Log LOG = LogFactory.getLog(TajoQueryEngine.class);

  private final FileSystem defaultFS;
  private final StorageManager storageManager;

  private final Path basePath;
  private final Path dataPath;

  private final PhysicalPlanner phyPlanner;

  public TajoQueryEngine(TajoConf conf) throws IOException {
    // Get the tajo base dir
    this.basePath = new Path(conf.getVar(ConfVars.ENGINE_BASE_DIR));
    LOG.info("Base dir is set " + basePath);
    
    // Get default DFS uri from the base dir
    this.defaultFS = basePath.getFileSystem(conf);
    LOG.info("FileSystem (" + this.defaultFS.getUri() + ") is initialized.");

    this.dataPath = new Path(conf.getVar(ConfVars.ENGINE_DATA_DIR));
    LOG.info("Tajo data dir is set " + dataPath);
        
    this.storageManager = new StorageManager(conf);
    this.phyPlanner = new PhysicalPlannerImpl(conf, storageManager);
  }
  
  public PhysicalExec createPlan(TaskAttemptContext ctx, LogicalNode plan)
      throws InternalException {
    return phyPlanner.createPlan(ctx, plan);
  }
  
  public void stop() throws IOException {
    this.defaultFS.close();
  }
}
