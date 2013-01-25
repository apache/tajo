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

package tajo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.LocalContainerLauncher;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JHAdminConfig;
import org.apache.hadoop.mapreduce.v2.jobhistory.JobHistoryUtils;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.service.Service;
import tajo.conf.TajoConf;
import tajo.pullserver.PullServerAuxService;

import java.io.File;
import java.io.IOException;

/**
 * Configures and starts the Tajo-specific components in the YARN cluster.
 *
 */
public class MiniTajoYarnCluster extends MiniYARNCluster {

  public static final String APPJAR = JarFinder
      .getJar(LocalContainerLauncher.class);

  private static final Log LOG = LogFactory.getLog(MiniTajoYarnCluster.class);

  public MiniTajoYarnCluster(String testName) {
    this(testName, 1);
  }

  public MiniTajoYarnCluster(String testName, int noOfNMs) {
    super(testName, noOfNMs, 1, 1);
  }

  @Override
  public void init(Configuration conf) {
    conf.set(MRConfig.FRAMEWORK_NAME, MRConfig.YARN_FRAMEWORK_NAME);
    if (conf.get(MRJobConfig.MR_AM_STAGING_DIR) == null) {
      conf.set(MRJobConfig.MR_AM_STAGING_DIR, new File(getTestWorkDir(),
          "apps_staging_dir/").getAbsolutePath());
    }
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,  "000");

    try {
      Path stagingPath = FileContext.getFileContext(conf).makeQualified(
          new Path(conf.get(MRJobConfig.MR_AM_STAGING_DIR)));
      FileContext fc=FileContext.getFileContext(stagingPath.toUri(), conf);
      if (fc.util().exists(stagingPath)) {
        LOG.info(stagingPath + " exists! deleting...");
        fc.delete(stagingPath, true);
      }
      LOG.info("mkdir: " + stagingPath);
      //mkdir the staging directory so that right permissions are set while running as proxy user
      fc.mkdir(stagingPath, null, true);
      //mkdir done directory as well
      String doneDir = JobHistoryUtils
          .getConfiguredHistoryServerDoneDirPrefix(conf);
      Path doneDirPath = fc.makeQualified(new Path(doneDir));
      fc.mkdir(doneDirPath, null, true);
    } catch (IOException e) {
      throw new YarnException("Could not create staging directory. ", e);
    }
    conf.set(MRConfig.MASTER_ADDRESS, "test"); // The default is local because of
    // which shuffle doesn't happen
    //configure the shuffle service in NM
    conf.setStrings(YarnConfiguration.NM_AUX_SERVICES, PullServerAuxService.PULLSERVER_SERVICEID);
    conf.setClass(String.format(YarnConfiguration.NM_AUX_SERVICE_FMT,
        PullServerAuxService.PULLSERVER_SERVICEID), PullServerAuxService.class,
        Service.class);

    // Non-standard shuffle port
    conf.setInt(TajoConf.ConfVars.PULLSERVER_PORT.name(), 0);

    conf.setClass(YarnConfiguration.NM_CONTAINER_EXECUTOR,
        DefaultContainerExecutor.class, ContainerExecutor.class);

    // TestMRJobs is for testing non-uberized operation only; see TestUberAM
    // for corresponding uberized tests.
    conf.setBoolean(MRJobConfig.JOB_UBERTASK_ENABLE, false);

    super.init(conf);
  }

  @Override
  public void start() {
    super.start();

    LOG.info("MiniTajoYarn NM Local Dir: " + getConfig().get(YarnConfiguration.NM_LOCAL_DIRS));
  }

  private class JobHistoryServerWrapper extends AbstractService {
    public JobHistoryServerWrapper() {
      super(JobHistoryServerWrapper.class.getName());
    }

    @Override
    public synchronized void start() {
      try {
        if (!getConfig().getBoolean(
            JHAdminConfig.MR_HISTORY_MINICLUSTER_FIXED_PORTS,
            JHAdminConfig.DEFAULT_MR_HISTORY_MINICLUSTER_FIXED_PORTS)) {
          // pick free random ports.
          getConfig().set(JHAdminConfig.MR_HISTORY_ADDRESS,
              MiniYARNCluster.getHostname() + ":0");
          getConfig().set(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS,
              MiniYARNCluster.getHostname() + ":0");
        }
        super.start();
      } catch (Throwable t) {
        throw new YarnException(t);
      }

      LOG.info("MiniMRYARN ResourceManager address: " +
          getConfig().get(YarnConfiguration.RM_ADDRESS));
      LOG.info("MiniMRYARN ResourceManager web address: " +
          getConfig().get(YarnConfiguration.RM_WEBAPP_ADDRESS));
      LOG.info("MiniMRYARN HistoryServer address: " +
          getConfig().get(JHAdminConfig.MR_HISTORY_ADDRESS));
      LOG.info("MiniMRYARN HistoryServer web address: " +
          getConfig().get(JHAdminConfig.MR_HISTORY_WEBAPP_ADDRESS));
    }

    @Override
    public synchronized void stop() {
      super.stop();
    }
  }

  public static void main(String [] args) {
    MiniTajoYarnCluster cluster = new MiniTajoYarnCluster(MiniTajoYarnCluster.class.getName());
    cluster.init(new TajoConf());
    cluster.start();
  }
}

