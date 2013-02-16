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

package tajo.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
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
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.ProtoUtils;
import org.apache.hadoop.yarn.util.Records;
import tajo.QueryConf;
import tajo.conf.TajoConf;
import tajo.master.QueryMaster.QueryContext;
import tajo.master.TaskRunnerEvent.EventType;
import tajo.master.event.QueryEvent;
import tajo.master.event.QueryEventType;
import tajo.master.event.TaskRunnerLaunchEvent;
import tajo.pullserver.PullServerAuxService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunnerLauncherImpl extends AbstractService implements TaskRunnerLauncher {

  /** Class Logger */
  private static final Log LOG = LogFactory.getLog(TaskRunnerLauncherImpl.class);
  private final YarnRPC yarnRPC;
  private final static RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private QueryContext context;
  private final String taskListenerHost;
  private final int taskListenerPort;

  // For ContainerLauncherSpec
  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  private static final Object classpathLock = new Object();
  private Object commonContainerSpecLock = new Object();
  private ContainerLaunchContext commonContainerSpec = null;

  final public static FsPermission QUERYCONF_FILE_PERMISSION =
      FsPermission.createImmutable((short) 0644); // rw-r--r--

  public TaskRunnerLauncherImpl(QueryContext context) {
    super(TaskRunnerLauncherImpl.class.getName());
    this.context = context;
    taskListenerHost = context.getTaskListener().getHostName();
    taskListenerPort = context.getTaskListener().getPort();
    yarnRPC = context.getYarnRPC();
  }

  public void start() {
    super.start();
  }

  public void stop() {
    super.stop();
  }

  @Override
  public void handle(TaskRunnerEvent event) {

    if (event.getType() == EventType.CONTAINER_REMOTE_LAUNCH) {
      TaskRunnerLaunchEvent castEvent = (TaskRunnerLaunchEvent) event;
      try {
        Container container = new Container(castEvent.getContainerId(),
            castEvent.getContainerMgrAddress(), castEvent.getContainerToken());
        container.launch(castEvent);

      } catch (Throwable t) {
        t.printStackTrace();
      }
    } else if (event.getType() == EventType.CONTAINER_REMOTE_CLEANUP) {
      try {
        if (context.containsContainer(event.getContainerId())) {
          context.getContainer(event.getContainerId()).kill();
        } else {
          LOG.error("No Such Container: " + event.getContainerId());
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * Lock this on initialClasspath so that there is only one fork in the AM for
   * getting the initial class-path. TODO: We already construct
   * a parent CLC and use it for all the containers, so this should go away
   * once the mr-generated-classpath stuff is gone.
   */
  private static String getInitialClasspath(Configuration conf) {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<>();

      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

  private ContainerLaunchContext createCommonContainerLaunchContext() {
    TajoConf conf = (TajoConf) getConfig();

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

    Map<String, String> environment = new HashMap<>();
    //String initialClassPath = getInitialClasspath(conf);
    environment.put(Environment.SHELL.name(), "/bin/bash");
    environment.put(Environment.JAVA_HOME.name(), System.getenv(Environment.JAVA_HOME.name()));

    // TODO - to be improved with tajo.sh shell script
    Properties prop = System.getProperties();
    if (prop.getProperty("tajo.test", "FALSE").equals("TRUE")) {
      environment.put(Environment.CLASSPATH.name(), prop.getProperty(
          "java.class.path", null));
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
      environment.put("HADOOP_HOME", System.getenv("HADOOP_HOME"));
      environment.put(
          Environment.HADOOP_COMMON_HOME.name(),
          System.getenv("HADOOP_HOME"));
      environment.put(
          Environment.HADOOP_HDFS_HOME.name(),
          System.getenv("HADOOP_HOME"));
      environment.put(
          Environment.HADOOP_YARN_HOME.name(),
          System.getenv("HADOOP_HOME"));
      environment.put("TAJO_BASE_CLASSPATH", System.getenv("TAJO_BASE_CLASSPATH"));
      environment.put(Environment.CLASSPATH.name(), classPathEnv.toString());
    }

    ctx.setEnvironment(environment);

    ////////////////////////////////////////////////////////////////////////////
    // Set the local resources
    ////////////////////////////////////////////////////////////////////////////
    Map<String, LocalResource> localResources = new HashMap<>();
    FileSystem fs = null;


    LOG.info("defaultFS: " + conf.get("fs.default.name"));
    LOG.info("defaultFS: " + conf.get("fs.defaultFS"));
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }

    FileContext fsCtx = null;
    try {
      fsCtx = FileContext.getFileContext(getConfig());
    } catch (UnsupportedFileSystemException e) {
      e.printStackTrace();
    }

    LOG.info("Writing a QueryConf to HDFS and add to local environment");
    Path queryConfPath = new Path(fs.getHomeDirectory(), QueryConf.FILENAME);
    try {
      writeConf(conf, queryConfPath);

      LocalResource queryConfSrc = createApplicationResource(fsCtx,
          queryConfPath, LocalResourceType.FILE);
      localResources.put(QueryConf.FILENAME,  queryConfSrc);

      ctx.setLocalResources(localResources);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Add shuffle token
    Map<String, ByteBuffer> serviceData = new HashMap<>();
    try {
      //LOG.info("Putting shuffle token in serviceData");
      serviceData.put(PullServerAuxService.PULLSERVER_SERVICEID,
          PullServerAuxService.serializeMetaData(0));
    } catch (IOException ioe) {
      LOG.error(ioe);
    }
    ctx.setServiceData(serviceData);

    return ctx;
  }


  public ContainerLaunchContext createContainerLaunchContext(TaskRunnerLaunchEvent event) {
    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec = createCommonContainerLaunchContext();
      }
    }

    // Setup environment by cloning from common env.
    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<>(env.size());
    myEnv.putAll(env);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<>();
    for (Map.Entry<String, ByteBuffer> entry : commonContainerSpec
        .getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Set the local resources
    ////////////////////////////////////////////////////////////////////////////
    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<>(30);

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
    vargs.add("tajo.worker.TaskRunner");
    vargs.add(taskListenerHost); // tasklistener hostname
    vargs.add(String.valueOf(taskListenerPort)); // tasklistener hostname
    vargs.add(event.getSubQueryId().toString()); // subqueryId
    vargs.add(event.getContainerMgrAddress()); // nodeId
    vargs.add(event.getContainerId().toString()); // containerId

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    LOG.info("Completed setting up taskrunner command " + command.toString());
    List<String> commands = new ArrayList<>();
    commands.add(command.toString());

    return BuilderUtils.newContainerLaunchContext(event.getContainerId(), commonContainerSpec.getUser(),
        event.getCapability(), commonContainerSpec.getLocalResources(), myEnv, commands, myServiceData,
        null, new HashMap<ApplicationAccessType, String>());
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

    ContainerManager proxy = user
        .doAs(new PrivilegedAction<ContainerManager>() {
          @Override
          public ContainerManager run() {
            return (ContainerManager) yarnRPC.getProxy(ContainerManager.class,
                cmAddr, getConfig());
          }
        });
    return proxy;
  }

  private LocalResource createApplicationResource(FileContext fs,
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

  private void writeConf(Configuration conf, Path queryConfFile)
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

  private static enum ContainerState {
    PREP, FAILED, RUNNING, DONE, KILLED_BEFORE_LAUNCH
  }

  public class Container {
    private ContainerState state;
    // store enough information to be able to cleanup the container
    private ContainerId containerID;
    final private String containerMgrAddress;
    private ContainerToken containerToken;
    private String hostName;
    private int port = -1;

    public Container(ContainerId containerID,
                     String containerMgrAddress, ContainerToken containerToken) {
      this.state = ContainerState.PREP;
      this.containerMgrAddress = containerMgrAddress;
      this.containerID = containerID;
      this.containerToken = containerToken;
    }

    public synchronized boolean isCompletelyDone() {
      return state == ContainerState.DONE || state == ContainerState.FAILED;
    }

    @SuppressWarnings("unchecked")
    public synchronized void launch(TaskRunnerLaunchEvent event) {
      LOG.info("Launching Container with Id: " + event.getContainerId());
      if(this.state == ContainerState.KILLED_BEFORE_LAUNCH) {
        state = ContainerState.DONE;
        LOG.error("Container (" + event.getContainerId()
            + " was killed before it was launched");
        return;
      }

      ContainerManager proxy = null;
      try {

        proxy = getCMProxy(containerID, containerMgrAddress,
            containerToken);

        // Construct the actual Container
        ContainerLaunchContext containerLaunchContext =
            createContainerLaunchContext(event);

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

        // after launching, send launched event to task attempt to move
        // it from ASSIGNED to RUNNING state
//      context.getEventHandler().handle(new AMContainerEventLaunched(containerID, port));

        // this is workaround code
        context.getEventHandler().handle(new QueryEvent(context.getQueryId(), QueryEventType.INIT_COMPLETED));

        this.state = ContainerState.RUNNING;
        this.hostName = event.getContainerMgrAddress().split(":")[0];
        context.addContainer(containerID, this);
      } catch (Throwable t) {
        String message = "Container launch failed for " + containerID + " : "
            + StringUtils.stringifyException(t);
        this.state = ContainerState.FAILED;
        LOG.error(message);
      } finally {
        if (proxy != null) {
          yarnRPC.stopProxy(proxy, getConfig());
        }
      }
    }

    @SuppressWarnings("unchecked")
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
//          context.getEventHandler().handle(
//              new AMContainerEventStopFailed(containerID, message));
          LOG.warn(message);
          this.state = ContainerState.DONE;
          return;
        } finally {
          if (proxy != null) {
            yarnRPC.stopProxy(proxy, getConfig());
          }
        }
        this.state = ContainerState.DONE;
      }
    }

    public String getHostName() {
      return this.hostName;
    }

    public int getPullServerPort() {
      return this.port;
    }
  }
}
