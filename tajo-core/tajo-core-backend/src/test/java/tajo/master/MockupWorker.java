/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.master;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.net.NetUtils;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.QueryUnitAttemptId;
import tajo.TajoProtos.TaskAttemptState;
import tajo.catalog.statistics.TableStat;
import tajo.conf.TajoConf;
import tajo.engine.MasterWorkerProtos.*;
import tajo.engine.query.QueryUnitRequestImpl;
import tajo.ipc.AsyncWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import tajo.ipc.protocolrecords.QueryUnitRequest;
import tajo.master.cluster.MasterAddressTracker;
import tajo.rpc.NullCallback;
import tajo.rpc.ProtoAsyncRpcClient;
import tajo.rpc.ProtoAsyncRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public abstract class MockupWorker
    extends Thread implements AsyncWorkerProtocol {
  public static enum Type {
    NORMAL,
    ABORT,
    SHUTDOWN
  }

  protected final static Log LOG = LogFactory.getLog(MockupWorker.class);
  protected final static int MAX_TASK_NUM = 120;

  protected final TajoConf conf;
  protected ProtoAsyncRpcServer rpcServer;
  protected InetSocketAddress isa;
  protected String serverName;

  protected ZkClient zkClient;
  protected MasterAddressTracker masterAddrTracker;
  protected ProtoAsyncRpcClient client;
  protected MasterWorkerProtocolService.Interface master;

  protected final Type type;

  protected Map<QueryUnitAttemptId, MockupTask> taskMap;
  protected List<MockupTask> taskQueue;
  protected boolean stopped;

  public MockupWorker(TajoConf conf, Type type) {
    this.conf = conf;
    this.type = type;
    taskMap = Maps.newHashMap();
    taskQueue = Lists.newArrayList();
    stopped = false;
  }

  protected void prepareServing() throws Exception {
    String hostname = DNS.getDefaultHost(
        conf.get("nta.master.dns.interface", "default"),
        conf.get("nta.master.dns.nameserver", "default"));
    int port = this.conf.getIntVar(TajoConf.ConfVars.LEAFSERVER_PORT);

    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + this.isa);
    }
    this.rpcServer = new ProtoAsyncRpcServer(AsyncWorkerProtocol.class,
        this, initialIsa);
    this.rpcServer.start();

    this.isa = this.rpcServer.getBindAddress();
    this.serverName = this.isa.getHostName() + ":" + this.isa.getPort();

    this.zkClient = new ZkClient(this.conf);
  }

  protected void participateCluster() throws Exception, KeeperException {
    this.masterAddrTracker = new MasterAddressTracker(zkClient);
    this.masterAddrTracker.start();

    byte[] master;
    do {
      master = masterAddrTracker.blockUntilAvailable(500);
      LOG.info("Waiting for the Tajo master.....");
    } while (master == null);

    LOG.info("Got the master address (" + new String(master) + ")");
    // if the znode already exists, it will be updated for notification.
    ZkUtil.upsertEphemeralNode(zkClient,
        ZkUtil.concat(NConstants.ZNODE_WORKERS, serverName));
    LOG.info("Created the znode " + NConstants.ZNODE_WORKERS + "/"
        + serverName);

    InetSocketAddress addr = NetUtils.createSocketAddr(new String(master));
    this.client = new ProtoAsyncRpcClient(MasterWorkerProtocol.class, addr);
    this.master = client.getStub();
  }

  public MasterWorkerProtocolService.Interface getMaster() {
    return this.master;
  }

  public String getServerName() {
    return this.serverName;
  }

  public Type getType() {
    return this.type;
  }

  public TaskStatusProto getReport(QueryUnitAttemptId queryUnitId,
                                         TaskAttemptState status) {
    TaskStatusProto.Builder builder = TaskStatusProto.newBuilder();
    builder.setId(queryUnitId.getProto())
        .setProgress(0.0f)
        .setState(status);

    if (status == TaskAttemptState.TA_SUCCEEDED) {
      builder.setResultStats(new TableStat().getProto());
    }

    return builder.build();
  }

  @Override
  public BoolProto requestQueryUnit(QueryUnitRequestProto proto) throws Exception {
    QueryUnitRequest request = new QueryUnitRequestImpl(proto);
    MockupTask task = new MockupTask(request.getId(), master, 9000);
    if (taskMap.containsKey(task.getId())) {
      throw new IllegalStateException("Query unit (" + task.getId() + ") is already is submitted");
    }
    taskMap.put(task.getId(), task);
    taskQueue.add(task);
    return null;
  }

  @Override
  public CommandResponseProto requestCommand(CommandRequestProto request) {
    QueryUnitAttemptId uid;
    for (Command cmd : request.getCommandList()) {
      uid = new QueryUnitAttemptId(cmd.getId());
      MockupTask task = this.taskMap.get(uid);
      TaskAttemptState status = task.getState();
      switch (cmd.getType()) {
        case FINALIZE:
          if (status == TaskAttemptState.TA_SUCCEEDED
              || status == TaskAttemptState.TA_FAILED
              || status == TaskAttemptState.TA_KILLED) {
            taskMap.remove(task.getId());
          } else {
            taskMap.remove(task.getId());
          }
          break;
        case STOP:
          taskMap.remove(task.getId());
          break;
        default:
          break;
      }
    }
    return null;
  }

  @Override
  public ServerStatusProto getServerStatus(PrimitiveProtos.NullProto request) {
    // serverStatus builder
    ServerStatusProto.Builder serverStatus = ServerStatusProto.newBuilder();

    // system(CPU, memory) status builder
    ServerStatusProto.System.Builder systemStatus = ServerStatusProto.System
        .newBuilder();

    systemStatus.setAvailableProcessors(Runtime.getRuntime()
        .availableProcessors());
    systemStatus.setFreeMemory(Runtime.getRuntime().freeMemory());
    systemStatus.setMaxMemory(Runtime.getRuntime().maxMemory());
    systemStatus.setTotalMemory(Runtime.getRuntime().totalMemory());

    serverStatus.setSystem(systemStatus);

    // disk status builder
    File[] roots = File.listRoots();
    for (File root : roots) {
      ServerStatusProto.Disk.Builder diskStatus = ServerStatusProto.Disk
          .newBuilder();

      diskStatus.setAbsolutePath(root.getAbsolutePath());
      diskStatus.setTotalSpace(root.getTotalSpace());
      diskStatus.setFreeSpace(root.getFreeSpace());
      diskStatus.setUsableSpace(root.getUsableSpace());

      serverStatus.addDisk(diskStatus);
    }
    return serverStatus.build();
  }

  @Override
  public void abort(String reason, Throwable cause) {
    if (cause != null) {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason, cause);
    } else {
      LOG.fatal("ABORTING leaf server " + this + ": " + reason);
    }
    // TODO - abortRequest : to be implemented
    shutdown(reason);
  }

  @Override
  public void shutdown(String why) {
    this.stopped = true;
    LOG.info("STOPPED: " + why);
    synchronized (this) {
      notifyAll();
    }
  }

  protected void progressTask() {
    if (taskQueue.size() > 0) {
      MockupTask task = taskQueue.get(0);
      switch (task.getState()) {
        case TA_PENDING:
          task.setState(TaskAttemptState.TA_RUNNING);
          break;
        case TA_RUNNING:
          task.updateTime(3000);
          if (task.getLeftTime() <= 0) {
            task.setState(TaskAttemptState.TA_SUCCEEDED);
            taskQueue.remove(0);
          }
          break;
        default:
          LOG.error("Invalid task status: " + task.getState());
          break;
      }
    }
  }

  protected void sendHeartbeat(long time) throws IOException {
    StatusReportProto.Builder ping = StatusReportProto.newBuilder();
    ping.setTimestamp(time);
    ping.setServerName(serverName);

    // to send
    TaskStatusProto status;
    int runningTasksNum = 0;

    // builds one status for each in-progress query
    for (MockupTask task : taskMap.values()) {
      if (task.getState() == TaskAttemptState.TA_FAILED
          || task.getState() == TaskAttemptState.TA_KILLED
          || task.getState() == TaskAttemptState.TA_SUCCEEDED) {
        // TODO - in-progress queries should be kept until this leafserver
        //tobeRemoved.add(task.getId());
      }

      status = this.getReport(task.getId(), task.getState());
      master.statusUpdate(null, status, NullCallback.get());
    }
  }

  protected void clear() {
    // remove the znode
    ZkUtil.concat(NConstants.ZNODE_WORKERS, serverName);

    rpcServer.shutdown();
    masterAddrTracker.stop();
    zkClient.close();
  }
}
