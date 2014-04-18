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

package org.apache.tajo.master.rm;

import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.YarnContainerProxy;
import org.apache.tajo.master.querymaster.QueryInProgress;
import org.apache.tajo.master.querymaster.QueryJobEvent;
import org.apache.tajo.util.ApplicationIdUtils;
import org.apache.tajo.worker.TajoWorker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.*;

public class YarnTajoResourceManager implements WorkerResourceManager {
  private static final Log LOG = LogFactory.getLog(YarnTajoResourceManager.class);

  private YarnClient yarnClient;
  private ApplicationMasterProtocol rmClient;
  private final RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
  private Configuration conf;
  private TajoMaster.MasterContext masterContext;

  public YarnTajoResourceManager() {

  }

  public YarnTajoResourceManager(TajoMaster.MasterContext masterContext) {
    this.masterContext = masterContext;
  }

  @Override
  public void stop() {
  }

  public Map<String, WorkerResource> getWorkers() {
    return new HashMap<String, WorkerResource>();
  }

  public Collection<String> getQueryMasters() {
    return new ArrayList<String>();
  }

  public TajoMasterProtocol.ClusterResourceSummary getClusterResourceSummary() {
    return TajoMasterProtocol.ClusterResourceSummary.newBuilder()
        .setNumWorkers(0)
        .setTotalCpuCoreSlots(0)
        .setTotalDiskSlots(0)
        .setTotalMemoryMB(0)
        .setTotalAvailableCpuCoreSlots(0)
        .setTotalAvailableDiskSlots(0)
        .setTotalAvailableMemoryMB(0)
        .build();
  }

  @Override
  public void workerHeartbeat(TajoMasterProtocol.TajoHeartbeat request) {
    throw new UnimplementedException("workerHeartbeat");
  }

  @Override
  public void releaseWorkerResource(ExecutionBlockId ebId, YarnProtos.ContainerIdProto containerId) {
    throw new UnimplementedException("releaseWorkerResource");
  }

  @Override
  public WorkerResource allocateQueryMaster(QueryInProgress queryInProgress) {
    throw new UnimplementedException("allocateQueryMaster");
  }

  @Override
  public void allocateWorkerResources(
      TajoMasterProtocol.WorkerResourceAllocationRequest request,
      RpcCallback<TajoMasterProtocol.WorkerResourceAllocationResponse> rpcCallBack) {
    throw new UnimplementedException("allocateWorkerResources");
  }

  @Override
  public void startQueryMaster(QueryInProgress queryInProgress) {
    try {
      allocateAndLaunchQueryMaster(queryInProgress);

      queryInProgress.getEventHandler().handle(
          new QueryJobEvent(QueryJobEvent.Type.QUERY_JOB_START, queryInProgress.getQueryInfo()));
    } catch (IOException e) {
      LOG.error(e);
    } catch (YarnException e) {
      LOG.error(e);
    }
  }

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    connectYarnClient();

    final YarnConfiguration yarnConf = new YarnConfiguration(conf);
    final YarnRPC rpc = YarnRPC.create(conf);
    final InetSocketAddress rmAddress = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    rmClient = currentUser.doAs(new PrivilegedAction<ApplicationMasterProtocol>() {
      @Override
      public ApplicationMasterProtocol run() {
        return (ApplicationMasterProtocol) rpc.getProxy(ApplicationMasterProtocol.class, rmAddress, yarnConf);
      }
    });
  }

  @Override
  public String getSeedQueryId() throws IOException {
    try {
      YarnClientApplication app = yarnClient.createApplication();
      return app.getApplicationSubmissionContext().getApplicationId().toString();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);

      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public void stopQueryMaster(QueryId queryId) {
    try {
      FinalApplicationStatus appStatus = FinalApplicationStatus.UNDEFINED;
      QueryInProgress queryInProgress = masterContext.getQueryJobManager().getQueryInProgress(queryId);
      if(queryInProgress == null) {
        return;
      }
      TajoProtos.QueryState state = queryInProgress.getQueryInfo().getQueryState();
      if (state == TajoProtos.QueryState.QUERY_SUCCEEDED) {
        appStatus = FinalApplicationStatus.SUCCEEDED;
      } else if (state == TajoProtos.QueryState.QUERY_FAILED || state == TajoProtos.QueryState.QUERY_ERROR) {
        appStatus = FinalApplicationStatus.FAILED;
      } else if (state == TajoProtos.QueryState.QUERY_ERROR) {
        appStatus = FinalApplicationStatus.FAILED;
      }
      FinishApplicationMasterRequest request = recordFactory
          .newRecordInstance(FinishApplicationMasterRequest.class);
      request.setFinalApplicationStatus(appStatus);
      request.setDiagnostics("QueryMaster shutdown by TajoMaster.");
      rmClient.finishApplicationMaster(request);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  private void connectYarnClient() {
    this.yarnClient = new YarnClientImpl();
    this.yarnClient.init(conf);
    this.yarnClient.start();
  }

  private ApplicationAttemptId allocateAndLaunchQueryMaster(QueryInProgress queryInProgress) throws IOException, YarnException {
    QueryId queryId = queryInProgress.getQueryId();
    ApplicationId appId = ApplicationIdUtils.queryIdToAppId(queryId);

    LOG.info("Allocate and launch ApplicationMaster for QueryMaster: queryId=" +
        queryId + ", appId=" + appId);

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
        YarnContainerProxy.createCommonContainerLaunchContext(masterContext.getConf(), queryId.toString(), true);

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
    String jvmOptions = masterContext.getConf().get("tajo.rm.yarn.querymaster.jvm.option", "-Xmx2000m");

    for(String eachToken: jvmOptions.split((" "))) {
      vargs.add(eachToken);
    }
    // Set Remote Debugging
    //if (!context.getQuery().getSubQuery(event.getExecutionBlockId()).isLeafQuery()) {
    //vargs.add("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    //}
    // Set class name
    vargs.add(TajoWorker.class.getCanonicalName());
    vargs.add("qm");
    vargs.add(queryId.toString()); // queryId
    vargs.add(masterContext.getTajoMasterService().getBindAddress().getHostName() + ":" +
        masterContext.getTajoMasterService().getBindAddress().getPort());

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
    resource.setMemory(2000);
    resource.setVirtualCores(1);

    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();

    ContainerLaunchContext masterContainerContext = BuilderUtils.newContainerLaunchContext(
        commonContainerLaunchContext.getLocalResources(),
        myEnv,
        commands,
        myServiceData,
        null,
        new HashMap<ApplicationAccessType, String>(2)
    );

    appContext.setAMContainerSpec(masterContainerContext);

    LOG.info("Submitting QueryMaster to ResourceManager");
    yarnClient.submitApplication(appContext);

    ApplicationReport appReport = monitorApplication(appId, EnumSet.of(YarnApplicationState.ACCEPTED));
    ApplicationAttemptId attemptId = appReport.getCurrentApplicationAttemptId();

    LOG.info("Launching QueryMaster with appAttemptId: " + attemptId);

    return attemptId;
  }

  private ApplicationReport monitorApplication(ApplicationId appId,
                                               Set<YarnApplicationState> finalState) throws IOException, YarnException {

    long sleepTime = 100;
    int count = 1;
    while (true) {
      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      LOG.info("Got application report from ASM for" + ", appId="
          + appId.getId() + ", appAttemptId="
          + report.getCurrentApplicationAttemptId() + ", clientToken="
          + report.getClientToAMToken() + ", appDiagnostics="
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

  public boolean isQueryMasterStopped(QueryId queryId) {
    ApplicationId appId = ApplicationIdUtils.queryIdToAppId(queryId);
    try {
      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      return EnumSet.of(
          YarnApplicationState.FINISHED,
          YarnApplicationState.KILLED,
          YarnApplicationState.FAILED).contains(state);
    } catch (YarnException e) {
      LOG.error(e.getMessage(), e);
      return false;
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      return false;
    }
  }
}
