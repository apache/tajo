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

package org.apache.tajo.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.pullserver.PullServerAuxService;
import org.apache.tajo.worker.TajoWorker;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.*;

public class YarnContainerProxy extends ContainerProxy {
  private final static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);

  protected final YarnRPC yarnRPC;
  final protected String containerMgrAddress;
  protected Token containerToken;

  public YarnContainerProxy(QueryMasterTask.QueryMasterTaskContext context, Configuration conf, YarnRPC yarnRPC,
                                  Container container, ExecutionBlockId executionBlockId) {
    super(context, conf, executionBlockId, container);
    this.yarnRPC = yarnRPC;

    NodeId nodeId = container.getNodeId();
    this.containerMgrAddress = nodeId.getHost() + ":" + nodeId.getPort();
    this.containerToken = container.getContainerToken();
  }

  protected ContainerManagementProtocol getCMProxy(ContainerId containerID,
                                                   final String containerManagerBindAddr,
                                                   Token containerToken)
      throws IOException {
    String [] hosts = containerManagerBindAddr.split(":");
    final InetSocketAddress cmAddr =
        new InetSocketAddress(hosts[0], Integer.parseInt(hosts[1]));
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      org.apache.hadoop.security.token.Token<ContainerTokenIdentifier> token =
          ConverterUtils.convertFromYarn(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManagementProtocol proxy = user.doAs(new PrivilegedAction<ContainerManagementProtocol>() {
      @Override
      public ContainerManagementProtocol run() {
        return (ContainerManagementProtocol) yarnRPC.getProxy(ContainerManagementProtocol.class,
            cmAddr, conf);
      }
    });

    return proxy;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized void launch(ContainerLaunchContext commonContainerLaunchContext) {
    LOG.info("Launching Container with Id: " + containerID);
    if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
      state = ContainerState.DONE;
      LOG.error("Container (" + containerID + " was killed before it was launched");
      return;
    }

    ContainerManagementProtocol proxy = null;
    try {

      proxy = getCMProxy(containerID, containerMgrAddress,
          containerToken);

      // Construct the actual Container
      ContainerLaunchContext containerLaunchContext = createContainerLaunchContext(commonContainerLaunchContext);

      // Now launch the actual container
      List<StartContainerRequest> startRequestList = new ArrayList<StartContainerRequest>();
      StartContainerRequest startRequest = Records
          .newRecord(StartContainerRequest.class);
      startRequest.setContainerLaunchContext(containerLaunchContext);
      startRequestList.add(startRequest);
      StartContainersRequest startRequests = Records.newRecord(StartContainersRequest.class);
      startRequests.setStartContainerRequests(startRequestList);
      StartContainersResponse response = proxy.startContainers(startRequests);

      ByteBuffer portInfo = response.getAllServicesMetaData().get(PullServerAuxService.PULLSERVER_SERVICEID);

      if(portInfo != null) {
        port = PullServerAuxService.deserializeMetaData(portInfo);
      }

      LOG.info("PullServer port returned by ContainerManager for "
          + containerID + " : " + port);

      if(port < 0) {
        this.state = ContainerState.FAILED;
        throw new IllegalStateException("Invalid shuffle port number "
            + port + " returned for " + containerID);
      }

      this.state = ContainerState.RUNNING;
      this.hostName = containerMgrAddress.split(":")[0];
      context.getResourceAllocator().addContainer(containerID, this);
    } catch (Throwable t) {
      String message = "Container launch failed for " + containerID + " : "
          + StringUtils.stringifyException(t);
      this.state = ContainerState.FAILED;
      LOG.error(message);
    } finally {
      if (proxy != null) {
        yarnRPC.stopProxy(proxy, conf);
      }
    }
  }


  public ContainerLaunchContext createContainerLaunchContext(ContainerLaunchContext commonContainerLaunchContext) {
    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerLaunchContext.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Map.Entry<String, ByteBuffer> entry : commonContainerLaunchContext.getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

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
    //if (!context.getQuery().getSubQuery(event.getExecutionBlockId()).isLeafQuery()) {
    //vargs.add("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    //}
    // Set class name
    //vargs.add(getRunnerClass());
    vargs.add(TajoWorker.class.getCanonicalName());
    vargs.add("tr");     //workerMode
    vargs.add(getId()); // subqueryId
    vargs.add(containerMgrAddress); // nodeId
    vargs.add(containerID.toString()); // containerId
    Vector<CharSequence> taskParams = getTaskParams();
    if(taskParams != null) {
      vargs.addAll(taskParams);
    }

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up TaskRunner command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    return BuilderUtils.newContainerLaunchContext(commonContainerLaunchContext.getLocalResources(),
        myEnv,
        commands,
        myServiceData,
        null,
        new HashMap<ApplicationAccessType, String>());
  }

  public static ContainerLaunchContext createCommonContainerLaunchContext(Configuration config,
                                                                          String queryId, boolean isMaster) {
    TajoConf conf = (TajoConf)config;

    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

    try {
      ByteBuffer userToken = ByteBuffer.wrap(UserGroupInformation.getCurrentUser().getShortUserName().getBytes());
      ctx.setTokens(userToken);
    } catch (IOException e) {
      e.printStackTrace();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Set the env variables to be setup
    ////////////////////////////////////////////////////////////////////////////
    LOG.info("Set the environment for the application master");

    Map<String, String> environment = new HashMap<String, String>();
    //String initialClassPath = getInitialClasspath(conf);
    environment.put(ApplicationConstants.Environment.SHELL.name(), "/bin/bash");
    if(System.getenv(ApplicationConstants.Environment.JAVA_HOME.name()) != null) {
      environment.put(ApplicationConstants.Environment.JAVA_HOME.name(), System.getenv(ApplicationConstants.Environment.JAVA_HOME.name()));
    }

    // TODO - to be improved with org.apache.tajo.sh shell script
    Properties prop = System.getProperties();

    if (prop.getProperty("tajo.test", "FALSE").equalsIgnoreCase("TRUE") ||
        (System.getenv("tajo.test") != null && System.getenv("tajo.test").equalsIgnoreCase("TRUE"))) {
      LOG.info("tajo.test is TRUE");
      environment.put(ApplicationConstants.Environment.CLASSPATH.name(), prop.getProperty("java.class.path", null));
      environment.put("tajo.test", "TRUE");
    } else {
      // Add AppMaster.jar location to classpath
      // At some point we should not be required to add
      // the hadoop specific classpaths to the env.
      // It should be provided out of the box.
      // For now setting all required classpaths including
      // the classpath to "." for the application jar
      StringBuilder classPathEnv = new StringBuilder("./");
      //for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) {
      for (String c : YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH) {
        classPathEnv.append(':');
        classPathEnv.append(c.trim());
      }

      classPathEnv.append(":" + System.getenv("TAJO_BASE_CLASSPATH"));
      classPathEnv.append(":./log4j.properties:./*");
      if(System.getenv("HADOOP_HOME") != null) {
        environment.put("HADOOP_HOME", System.getenv("HADOOP_HOME"));
        environment.put(
            ApplicationConstants.Environment.HADOOP_COMMON_HOME.name(),
            System.getenv("HADOOP_HOME"));
        environment.put(
            ApplicationConstants.Environment.HADOOP_HDFS_HOME.name(),
            System.getenv("HADOOP_HOME"));
        environment.put(
            ApplicationConstants.Environment.HADOOP_YARN_HOME.name(),
            System.getenv("HADOOP_HOME"));
      }

      if(System.getenv("TAJO_BASE_CLASSPATH") != null) {
        environment.put("TAJO_BASE_CLASSPATH", System.getenv("TAJO_BASE_CLASSPATH"));
      }
      environment.put(ApplicationConstants.Environment.CLASSPATH.name(), classPathEnv.toString());
    }

    ctx.setEnvironment(environment);

    if(LOG.isDebugEnabled()) {
      LOG.debug("=================================================");
      for(Map.Entry<String, String> entry: environment.entrySet()) {
        LOG.debug(entry.getKey() + "=" + entry.getValue());
      }
      LOG.debug("=================================================");
    }
    ////////////////////////////////////////////////////////////////////////////
    // Set the local resources
    ////////////////////////////////////////////////////////////////////////////
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    LOG.info("defaultFS: " + conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));

    try {
      FileSystem fs = FileSystem.get(conf);
      FileContext fsCtx = FileContext.getFileContext(conf);
      Path systemConfPath = TajoConf.getSystemConfPath(conf);
      if (!fs.exists(systemConfPath)) {
        LOG.error("system_conf.xml (" + systemConfPath.toString() + ") Not Found");
      }
      LocalResource systemConfResource = createApplicationResource(fsCtx, systemConfPath, LocalResourceType.FILE);
      localResources.put(TajoConstants.SYSTEM_CONF_FILENAME, systemConfResource);
      ctx.setLocalResources(localResources);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();
    try {
      serviceData.put(PullServerAuxService.PULLSERVER_SERVICEID, PullServerAuxService.serializeMetaData(0));
    } catch (IOException ioe) {
      LOG.error(ioe);
    }
    ctx.setServiceData(serviceData);

    return ctx;
  }

  private static LocalResource createApplicationResource(FileContext fs,
                                                         Path p, LocalResourceType type)
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

  private static void writeConf(Configuration conf, Path queryConfFile)
      throws IOException {
    // Write job file to Tajo's fs
    FileSystem fs = queryConfFile.getFileSystem(conf);
    FSDataOutputStream out =
        FileSystem.create(fs, queryConfFile,
            new FsPermission(QUERYCONF_FILE_PERMISSION));
    try {
      conf.writeXml(out);
    } finally {
      out.close();
    }
  }

  @Override
  public synchronized void stopContainer() {

    if(isCompletelyDone()) {
      return;
    }
    if(this.state == ContainerState.PREP) {
      this.state = ContainerState.KILLED_BEFORE_LAUNCH;
    } else {
      LOG.info("KILLING " + containerID);

      ContainerManagementProtocol proxy = null;
      try {
        proxy = getCMProxy(this.containerID, this.containerMgrAddress,
            this.containerToken);

        // kill the remote container if already launched
        List<ContainerId> willBeStopedIds = new ArrayList<ContainerId>();
        willBeStopedIds.add(this.containerID);
        StopContainersRequest stopRequests = Records.newRecord(StopContainersRequest.class);
        stopRequests.setContainerIds(willBeStopedIds);
        proxy.stopContainers(stopRequests);
        // If stopContainer returns without an error, assuming the stop made
        // it over to the NodeManager.
//          context.getEventHandler().handle(
//              new AMContainerEvent(containerID, AMContainerEventType.C_NM_STOP_SENT));
        context.getResourceAllocator().removeContainer(containerID);
      } catch (Throwable t) {

        // ignore the cleanup failure
        String message = "cleanup failed for container "
            + this.containerID + " : "
            + StringUtils.stringifyException(t);
//          context.getEventHandler().handle(
//              new AMContainerEventStopFailed(containerID, message));
        LOG.warn(message);
        this.state = ContainerState.DONE;
        return;
      } finally {
        if (proxy != null) {
          yarnRPC.stopProxy(proxy, conf);
        }
      }
      this.state = ContainerState.DONE;
    }
  }

  protected Vector<CharSequence> getTaskParams() {
    String queryMasterHost = context.getQueryMasterContext().getWorkerContext()
        .getTajoWorkerManagerService().getBindAddr().getHostName();
    int queryMasterPort = context.getQueryMasterContext().getWorkerContext()
        .getTajoWorkerManagerService().getBindAddr().getPort();

    Vector<CharSequence> taskParams = new Vector<CharSequence>();
    taskParams.add(queryMasterHost); // queryMaster hostname
    taskParams.add(String.valueOf(queryMasterPort)); // queryMaster port
    taskParams.add(context.getStagingDir().toString());
    return taskParams;
  }
}
