/*
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

package tajo.master.rm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import tajo.TajoProtos.QueryState;
import tajo.master.Query;
import tajo.master.QueryMaster.QueryContext;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RMCommunicator extends AbstractService {
  private static final Log LOG = LogFactory.getLog(RMCommunicator.class);

  protected static final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);

  // Resource Manager RPC
  private YarnRPC rpc;
  protected AMRMProtocol scheduler;

  // For Query
  protected QueryContext context;
  protected Query query;
  private int rmPollInterval = 1000;//millis
  protected ApplicationId applicationId;
  protected ApplicationAttemptId applicationAttemptId;
  protected Map<ApplicationAccessType, String> applicationACLs;

  // RMCommunicator
  private final AtomicBoolean stopped;
  protected Thread allocatorThread;

  // resource
  private Resource minContainerCapability;
  private Resource maxContainerCapability;

  // Has a signal (SIGTERM etc) been issued?
  protected volatile boolean isSignalled = false;

  public RMCommunicator(QueryContext context) {
    super(RMCommunicator.class.getName());
    this.context = context;
    this.applicationId = context.getApplicationId();
    this.applicationAttemptId = context.getApplicationAttemptId();

    stopped = new AtomicBoolean(false);
  }

  @Override
  public void init(Configuration conf) {
    LOG.info("defaultFS: " + conf.get("fs.default.name"));
    LOG.info("defaultFS: " + conf.get("fs.defaultFS"));

    super.init(conf);
  }

  public void start() {
    this.query = context.getQuery();
    rpc = YarnRPC.create(getConfig());
    this.scheduler = createSchedulerProxy();


    AllocateRequest allocateRequest = BuilderUtils.newAllocateRequest(
        applicationAttemptId, 0, 0.0f,
        new ArrayList<ResourceRequest>(), new ArrayList<ContainerId>());
    try {
      AllocateResponse allocateResponse = scheduler.allocate(allocateRequest);
      context.setNumClusterNode(allocateResponse.getNumClusterNodes());
    } catch (YarnRemoteException e) {
      e.printStackTrace();
    }
    register();
    startAllocatorThread();
    super.start();
  }

  public void stop() {
    if (stopped.getAndSet(true)) {
      // return if already stopped
      return;
    }
    allocatorThread.interrupt();
    try {
      allocatorThread.join();
    } catch (InterruptedException ie) {
      LOG.warn("InterruptedException while stopping", ie);
    }
    unregister();
    super.stop();
  }

  protected void register() {
    //Register
    try {
      RegisterApplicationMasterRequest request =
          recordFactory.newRecordInstance(RegisterApplicationMasterRequest.class);
      request.setApplicationAttemptId(applicationAttemptId);
      LOG.info("Tracking Addr: " + context.getRpcAddress());
      request.setHost(context.getRpcAddress().getHostName());
      request.setRpcPort(context.getRpcAddress().getPort());
      // TODO - to be changed to http server
      //request.setTrackingUrl("http://" + NetUtils.getIpPortString(context.getRpcAddress()));
      request.setTrackingUrl("http://localhost:1234");
      RegisterApplicationMasterResponse response =
          scheduler.registerApplicationMaster(request);
      minContainerCapability = response.getMinimumResourceCapability();
      maxContainerCapability = response.getMaximumResourceCapability();
      context.setMaxContainerCapability(maxContainerCapability.getMemory());
      context.setMinContainerCapability(minContainerCapability.getMemory());
      this.applicationACLs = response.getApplicationACLs();
      LOG.info("minContainerCapability: " + minContainerCapability.getMemory());
      LOG.info("maxContainerCapability: " + maxContainerCapability.getMemory());
    } catch (Exception are) {
      LOG.error("Exception while registering", are);
      throw new YarnException(are);
    }
  }

  protected void unregister() {
    try {
      FinalApplicationStatus finishState = FinalApplicationStatus.UNDEFINED;
      if (query.getState() == QueryState.QUERY_SUCCEEDED) {
        finishState = FinalApplicationStatus.SUCCEEDED;
      } else if (query.getState() == QueryState.QUERY_KILLED
          || (query.getState() == QueryState.QUERY_RUNNING && isSignalled)) {
        finishState = FinalApplicationStatus.KILLED;
      } else if (query.getState() == QueryState.QUERY_FAILED
          || query.getState() == QueryState.QUERY_ERROR) {
        finishState = FinalApplicationStatus.FAILED;
      }
      StringBuffer sb = new StringBuffer();
//      for (String s : query.getDiagnostics()) {
//        sb.append(s).append("\n");
//      }
      LOG.info("Setting job diagnostics to " + sb.toString());

      // TODO - to be implemented
//      String historyUrl = JobHistoryUtils.getHistoryUrl(getConfig(),
//          context.getApplicationId());
//      LOG.info("History url is " + historyUrl);

      FinishApplicationMasterRequest request =
          recordFactory.newRecordInstance(FinishApplicationMasterRequest.class);
      request.setAppAttemptId(this.applicationAttemptId);
      request.setFinishApplicationStatus(finishState);
      request.setDiagnostics(""); // TODO - tobe implemented
      request.setTrackingUrl("");
      scheduler.finishApplicationMaster(request);
    } catch(Exception are) {
      LOG.error("Exception while unregistering ", are);
    }
  }

  protected Resource getMinContainerCapability() {
    return minContainerCapability;
  }

  protected Resource getMaxContainerCapability() {
    return maxContainerCapability;
  }

  public abstract void heartbeat() throws Exception;

  protected AMRMProtocol createSchedulerProxy() {
    final Configuration conf = getConfig();
    final InetSocketAddress serviceAddr = conf.getSocketAddr(
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    UserGroupInformation currentUser;
    try {
      currentUser = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      throw new YarnException(e);
    }

    if (UserGroupInformation.isSecurityEnabled()) {
      String tokenURLEncodedStr = System.getenv().get(
          ApplicationConstants.APPLICATION_MASTER_TOKEN_ENV_NAME);
      Token<? extends TokenIdentifier> token = new Token<TokenIdentifier>();

      try {
        token.decodeFromUrlString(tokenURLEncodedStr);
      } catch (IOException e) {
        throw new YarnException(e);
      }

      SecurityUtil.setTokenService(token, serviceAddr);
      if (LOG.isDebugEnabled()) {
        LOG.debug("AppMasterToken is " + token);
      }
      currentUser.addToken(token);
    }

    return currentUser.doAs(new PrivilegedAction<AMRMProtocol>() {
      @Override
      public AMRMProtocol run() {
        return (AMRMProtocol) rpc.getProxy(AMRMProtocol.class,
            serviceAddr, conf);
      }
    });
  }

  protected void startAllocatorThread() {
    allocatorThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            Thread.sleep(rmPollInterval);
            try {
              heartbeat();
            } catch (YarnException e) {
              LOG.error("Error communicating with RM: " + e.getMessage() , e);
              return;
            } catch (Exception e) {
              LOG.error("ERROR IN CONTACTING RM. ", e);
              // TODO: for other exceptions
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("Allocated thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });
    allocatorThread.setName("RMCommunicator Allocator");
    allocatorThread.start();
  }

  public void setSignalled(boolean isSignalled) {
    this.isSignalled = isSignalled;
    LOG.info("RMCommunicator notified that iSignalled is: "
        + isSignalled);
  }

  protected ContainerManager getCMProxy(ContainerId containerID,
                                        final String containerManagerBindAddr, ContainerToken containerToken)
      throws IOException {

    final InetSocketAddress cmAddr =
        NetUtils.createSocketAddr(containerManagerBindAddr);
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
          ProtoUtils.convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) rpc.getProxy(ContainerManager.class,
                cmAddr, getConfig());
          }
        });
    return proxy;
  }

  public static ContainerLaunchContext newContainerLaunchContext(
      ContainerId containerID, String user, Resource assignedCapability,
      Map<String, LocalResource> localResources,
      Map<String, String> environment, List<String> commands,
      Map<String, ByteBuffer> serviceData, ByteBuffer containerTokens,
      Map<ApplicationAccessType, String> acls) {
    ContainerLaunchContext container = recordFactory
        .newRecordInstance(ContainerLaunchContext.class);
    container.setContainerId(containerID);
    container.setUser(user);
    container.setResource(assignedCapability);
    container.setLocalResources(localResources);
    container.setEnvironment(environment);
    container.setCommands(commands);
    container.setServiceData(serviceData);
    container.setContainerTokens(containerTokens);
    container.setApplicationACLs(acls);
    return container;
  }

  private LocalResource createApplicationResource(FileContext fs, Path p, LocalResourceType type)
      throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }
}
