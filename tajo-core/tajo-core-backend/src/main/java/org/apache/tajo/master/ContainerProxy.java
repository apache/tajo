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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tajo.QueryConf;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.querymaster.QueryMaster;
import org.apache.tajo.pullserver.PullServerAuxService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.*;

public abstract class ContainerProxy {
  private static final Log LOG = LogFactory.getLog(ContainerProxy.class);

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
          FsPermission.createImmutable((short) 0644); // rw-r--r--

  private final static RecordFactory recordFactory =
          RecordFactoryProvider.getRecordFactory(null);

  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  private final YarnRPC yarnRPC;
  private Configuration conf;
  private QueryMaster.QueryContext context;

  private ContainerState state;
  // store enough information to be able to cleanup the container
  private Container container;
  private ContainerId containerID;
  final private String containerMgrAddress;
  private ContainerToken containerToken;
  private String hostName;
  private int port = -1;

  protected abstract void containerStarted();
  protected abstract String getId();
  protected abstract String getRunnerClass();
  protected abstract Vector<CharSequence> getTaskParams();

  public ContainerProxy(QueryMaster.QueryContext context, Configuration conf, YarnRPC yarnRPC, Container container) {
    this.context = context;
    this.conf = conf;
    this.yarnRPC = yarnRPC;
    this.state = ContainerState.PREP;
    this.container = container;
    this.containerID = container.getId();
    NodeId nodeId = container.getNodeId();
    this.containerMgrAddress = nodeId.getHost() + ":" + nodeId.getPort();
    this.containerToken = container.getContainerToken();
  }

  protected ContainerManager getCMProxy(ContainerId containerID,
                                        final String containerManagerBindAddr,
                                        ContainerToken containerToken)
          throws IOException {
    String [] hosts = containerManagerBindAddr.split(":");
    final InetSocketAddress cmAddr =
            new InetSocketAddress(hosts[0], Integer.parseInt(hosts[1]));
    UserGroupInformation user = UserGroupInformation.getCurrentUser();

    if (UserGroupInformation.isSecurityEnabled()) {
      Token<ContainerTokenIdentifier> token =
              ProtoUtils.convertFromProtoFormat(containerToken, cmAddr);
      // the user in createRemoteUser in this context has to be ContainerID
      user = UserGroupInformation.createRemoteUser(containerID.toString());
      user.addToken(token);
    }

    ContainerManager proxy = user.doAs(new PrivilegedAction<ContainerManager>() {
      @Override
      public ContainerManager run() {
        return (ContainerManager) yarnRPC.getProxy(ContainerManager.class, cmAddr, conf);
      }
    });

    return proxy;
  }

  public synchronized boolean isCompletelyDone() {
    return state == ContainerState.DONE || state == ContainerState.FAILED;
  }

  @SuppressWarnings("unchecked")
  public synchronized void launch(ContainerLaunchContext commonContainerLaunchContext) {
    LOG.info("Launching Container with Id: " + containerID);
    if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
      state = ContainerState.DONE;
      LOG.error("Container (" + containerID + " was killed before it was launched");
      return;
    }

    ContainerManager proxy = null;
    try {

      proxy = getCMProxy(containerID, containerMgrAddress,
              containerToken);

      // Construct the actual Container
      ContainerLaunchContext containerLaunchContext = createContainerLaunchContext(commonContainerLaunchContext);

      // Now launch the actual container
      StartContainerRequest startRequest = Records
              .newRecord(StartContainerRequest.class);
      startRequest.setContainerLaunchContext(containerLaunchContext);
      StartContainerResponse response = proxy.startContainer(startRequest);

      ByteBuffer portInfo = response
              .getServiceResponse(PullServerAuxService.PULLSERVER_SERVICEID);

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

      containerStarted();

      this.state = ContainerState.RUNNING;
      this.hostName = containerMgrAddress.split(":")[0];
      context.addContainer(containerID, this);
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

  public synchronized void kill() {

    if(isCompletelyDone()) {
      return;
    }
    if(this.state == ContainerState.PREP) {
      this.state = ContainerState.KILLED_BEFORE_LAUNCH;
    } else {
      LOG.info("KILLING " + containerID);

      ContainerManager proxy = null;
      try {
        proxy = getCMProxy(this.containerID, this.containerMgrAddress,
                this.containerToken);

        // kill the remote container if already launched
        StopContainerRequest stopRequest = Records
                .newRecord(StopContainerRequest.class);
        stopRequest.setContainerId(this.containerID);
        proxy.stopContainer(stopRequest);
        // If stopContainer returns without an error, assuming the stop made
        // it over to the NodeManager.
//          context.getEventHandler().handle(
//              new AMContainerEvent(containerID, AMContainerEventType.C_NM_STOP_SENT));
        context.removeContainer(containerID);
      } catch (Throwable t) {

        // ignore the cleanup failure
        String message = "cleanup failed for container "
                + this.containerID + " : "
                + StringUtils.stringifyException(t);
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

  public static ContainerLaunchContext createCommonContainerLaunchContext(Configuration config) {
    TajoConf conf = (TajoConf)config;

    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    try {
      ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName());
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
      environment.put(ApplicationConstants.Environment.JAVA_HOME.name(),
          System.getenv(ApplicationConstants.Environment.JAVA_HOME.name()));
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
        environment.put(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name(), System.getenv("HADOOP_HOME"));
        environment.put(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name(), System.getenv("HADOOP_HOME"));
        environment.put(ApplicationConstants.Environment.HADOOP_YARN_HOME.name(), System.getenv("HADOOP_HOME"));
      }

      if(System.getenv("TAJO_BASE_CLASSPATH") != null) {
        environment.put("TAJO_BASE_CLASSPATH", System.getenv("TAJO_BASE_CLASSPATH"));
      }
      environment.put(ApplicationConstants.Environment.CLASSPATH.name(), classPathEnv.toString());
    }

    ctx.setEnvironment(environment);

    ////////////////////////////////////////////////////////////////////////////
    // Set the local resources
    ////////////////////////////////////////////////////////////////////////////
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    FileSystem fs = null;

    LOG.info("defaultFS: " + conf.get("fs.defaultFS"));

    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    FileContext fsCtx = null;
    try {
      fsCtx = FileContext.getFileContext(conf);
    } catch (UnsupportedFileSystemException e) {
      LOG.error(e.getMessage(), e);
    }

    LOG.info("Writing a QueryConf to HDFS and add to local environment");

    Path queryConfPath = new Path(fs.getHomeDirectory(), QueryConf.FILENAME);
    try {
      writeConf(conf, queryConfPath);

      LocalResource queryConfSrc = createApplicationResource(fsCtx, queryConfPath, LocalResourceType.FILE);
      localResources.put(QueryConf.FILENAME,  queryConfSrc);

      ctx.setLocalResources(localResources);
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
    }

    // TODO - move to sub-class
    // Add shuffle token
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
    //if (!context.getQuery().getSubQuery(event.getSubQueryId()).isLeafQuery()) {
    //vargs.add("-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=5005");
    //}
    // Set class name
    vargs.add(getRunnerClass());
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

    return BuilderUtils.newContainerLaunchContext(containerID, commonContainerLaunchContext.getUser(),
            container.getResource(), commonContainerLaunchContext.getLocalResources(), myEnv, commands,
            myServiceData, null, new HashMap<ApplicationAccessType, String>());
  }

  public String getTaskHostName() {
    return this.hostName;
  }

  public int getTaskPort() {
    return this.port;
  }
}
