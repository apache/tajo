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

package org.apache.tajo.master.querymaster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.engine.planner.logical.LogicalRootNode;
import org.apache.tajo.ipc.QueryMasterManagerProtocol;
import org.apache.tajo.ipc.QueryMasterManagerProtocol.QueryHeartbeatResponse;
import org.apache.tajo.master.ContainerProxy;
import org.apache.tajo.master.TajoMaster;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

// TODO - check QueryMaster status and if QueryMaster failed, release resource
public class QueryMasterManager extends CompositeService {
  private static final Log LOG = LogFactory.getLog(QueryMasterManager.class.getName());

  // Master Context
  private final TajoMaster.MasterContext masterContext;

  // AppMaster Common
  private final Clock clock;
  private final long appSubmitTime;
  private final ApplicationId appId;
  private ApplicationAttemptId appAttemptId;

  protected YarnClient yarnClient;

  // For Query
  private final QueryId queryId;

  private AsyncDispatcher dispatcher;
  private YarnRPC rpc;

  private TajoProtos.QueryState state;
  private float progress;
  private long finishTime;
  private TableDesc resultDesc;
  private String queryMasterHost;
  private int queryMasterPort;
  private int queryMasterClientPort;

  private LogicalRootNode plan;

  private AtomicBoolean querySubmitted = new AtomicBoolean(false);

  private AtomicBoolean queryMasterStopped = new AtomicBoolean(true);

  private boolean stopCheckThreadStarted = false;

  private String query;

  public QueryMasterManager(final TajoMaster.MasterContext masterContext,
                     final YarnClient yarnClient,
                     final QueryId queryId,
                     final String query,
                     final LogicalRootNode plan,
                     final ApplicationId appId,
                     final Clock clock, long appSubmitTime) {
    super(QueryMasterManager.class.getName());
    this.masterContext = masterContext;
    this.yarnClient = yarnClient;

    this.appId = appId;
    this.clock = clock;
    this.appSubmitTime = appSubmitTime;
    this.queryId = queryId;
    this.plan = plan;
    this.query = query;
    LOG.info("Created Query Master Manager for AppId=" + appId + ", QueryID=" + queryId);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);

    state = TajoProtos.QueryState.QUERY_MASTER_INIT;
  }

  public TajoProtos.QueryState getState() {
    return state;
  }

  @Override
  public void start() {
    try {
      appAttemptId = allocateAndLaunchQueryMaster();
    } catch (YarnRemoteException e) {
      LOG.error(e.getMessage(), e);
    }
    super.start();
  }

  @Override
  public void stop() {
    while(true) {
      if(queryMasterStopped.get()) {
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    LOG.info("QueryMasterManager for " + queryId + " stopped");
    super.stop();
  }

  public float getProgress() {
    return progress;
  }

  public long getAppSubmitTime() {
    return appSubmitTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public TableDesc getResultDesc() {
    return resultDesc;
  }

  public String getQueryMasterHost() {
    return queryMasterHost;
  }

  public int getQueryMasterPort() {
    return queryMasterPort;
  }

  public int getQueryMasterClientPort() {
    return queryMasterClientPort;
  }

  public synchronized QueryHeartbeatResponse.ResponseCommand queryHeartbeat(QueryMasterManagerProtocol.QueryHeartbeat queryHeartbeat) {
    this.queryMasterHost = queryHeartbeat.getQueryMasterHost();
    this.queryMasterPort = queryHeartbeat.getQueryMasterPort();
    this.queryMasterClientPort = queryHeartbeat.getQueryMasterClientPort();
    this.state = queryHeartbeat.getState();
    if(state == TajoProtos.QueryState.QUERY_FAILED) {
      //TODO needed QueryMaster's detail status(failed before or after launching worker)
      queryMasterStopped.set(true);
      if(queryHeartbeat.getStatusMessage() != null) {
        LOG.warn(queryId + " failed, " + queryHeartbeat.getStatusMessage());
      }
    }

    if(!stopCheckThreadStarted && !queryMasterStopped.get() && isFinishState(this.state)) {
      stopCheckThreadStarted = true;
      startCheckingQueryMasterStop();
    }
    if(appAttemptId != null && !querySubmitted.get()) {
      LOG.info("submitQuery to QueryMaster(" + queryMasterHost + ":" + queryMasterPort + ")");
      queryMasterStopped.set(false);
      querySubmitted.set(true);
      List<String> params = new ArrayList<String>(3);
      params.add(appAttemptId.toString());
      params.add(query);
      params.add(plan.toJson());
      return QueryHeartbeatResponse.ResponseCommand.newBuilder()
          .setCommand("executeQuery")
          .addAllParams(params)
          .build();
    } else {
      return null;
    }
  }

  private boolean isFinishState(TajoProtos.QueryState state) {
    return state == TajoProtos.QueryState.QUERY_FAILED ||
        state == TajoProtos.QueryState.QUERY_KILLED ||
        state == TajoProtos.QueryState.QUERY_SUCCEEDED;
  }

  private void startCheckingQueryMasterStop() {
    Thread t = new Thread() {
      public void run() {
        try {
          ApplicationReport report = monitorApplication(appId,
              EnumSet.of(
                  YarnApplicationState.FINISHED,
                  YarnApplicationState.KILLED,
                  YarnApplicationState.FAILED));
          queryMasterStopped.set(true);
          LOG.info("QueryMaster (" + queryId + ") stopped");
        } catch (YarnRemoteException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    };

    t.start();
  }

  private ApplicationAttemptId allocateAndLaunchQueryMaster() throws YarnRemoteException {
    LOG.info("Allocate and launch QueryMaster:" + yarnClient);
    ApplicationSubmissionContext appContext = Records.newRecord(ApplicationSubmissionContext.class);

    // set the application id
    appContext.setApplicationId(appId);
    // set the application name
    appContext.setApplicationName("Tajo");

    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(5);
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");

    ContainerLaunchContext commonContainerLaunchContext =
            ContainerProxy.createCommonContainerLaunchContext(masterContext.getConf());

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerLaunchContext.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);

    ////////////////////////////////////////////////////////////////////////////
    // Set the local resources
    ////////////////////////////////////////////////////////////////////////////
    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    //LOG.info("Setting up app master command");
    vargs.add("${JAVA_HOME}" + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx2000m");
    // Set Remote Debugging
    //if (!context.getQuery().getSubQuery(event.getSubQueryId()).isLeafQuery()) {
    //vargs.add("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    //}
    // Set class name
    vargs.add(QueryMasterRunner.class.getCanonicalName());
    vargs.add(queryId.toString()); // queryId
    vargs.add(String.valueOf(appSubmitTime));
    vargs.add(masterContext.getQueryMasterManagerService().getBindAddress().getHostName() + ":" +
            masterContext.getQueryMasterManagerService().getBindAddress().getPort());

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up QueryMasterRunner command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    final Resource resource = Records.newRecord(Resource.class);
    // TODO - get default value from conf
    resource.setMemory(2048);
    resource.setVirtualCores(1);

    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();

    ContainerLaunchContext masterContainerContext = BuilderUtils.newContainerLaunchContext(
            null, commonContainerLaunchContext.getUser(),
            resource, commonContainerLaunchContext.getLocalResources(), myEnv, commands,
            myServiceData, null, new HashMap<ApplicationAccessType, String>(2));

    appContext.setAMContainerSpec(masterContainerContext);

    LOG.info("Submitting QueryMaster to ResourceManager");
    yarnClient.submitApplication(appContext);

    ApplicationReport appReport = monitorApplication(appId, EnumSet.of(YarnApplicationState.ACCEPTED));
    ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();

    LOG.info("Launching QueryMaster with id: " + attemptId);

    state = TajoProtos.QueryState.QUERY_MASTER_LAUNCHED;

    return attemptId;
  }

  private ApplicationReport monitorApplication(ApplicationId appId,
                                               Set<YarnApplicationState> finalState) throws YarnRemoteException {

    long sleepTime = 100;
    int count = 1;
    while (true) {
      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
              + appId.getId() + ", appAttemptId="
              + report.getCurrentApplicationAttemptId() + ", clientToken="
              + report.getClientToken() + ", appDiagnostics="
              + report.getDiagnostics() + ", appMasterHost=" + report.getHost()
              + ", appQueue=" + report.getQueue() + ", appMasterRpcPort="
              + report.getRpcPort() + ", appStartTime=" + report.getStartTime()
              + ", yarnAppState=" + report.getYarnApplicationState().toString()
              + ", distributedFinalState="
              + report.getFinalApplicationStatus().toString() + ", appTrackingUrl="
              + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      if (finalState.contains(state)) {
        return report;
      }
      try {
        Thread.sleep(sleepTime);
        sleepTime = count * 100;
        if(count < 10) {
          count++;
        }
      } catch (InterruptedException e) {
        //LOG.debug("Thread sleep in monitoring loop interrupted");
      }

    }
  }
}
