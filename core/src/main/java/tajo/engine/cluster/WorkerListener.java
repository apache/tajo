
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

package tajo.engine.cluster;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import tajo.QueryUnitAttemptId;
import tajo.common.Sleeper;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.InProgressStatusProto;
import tajo.engine.MasterWorkerProtos.PingRequestProto;
import tajo.engine.MasterWorkerProtos.PingResponseProto;
import tajo.engine.ipc.MasterInterface;
import tajo.engine.ipc.PingRequest;
import tajo.engine.planner.global.QueryUnitAttempt;
import tajo.engine.query.PingRequestImpl;
import tajo.master.TajoMaster;
import tajo.rpc.NettyRpc;
import tajo.rpc.NettyRpcServer;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

public class WorkerListener extends Thread implements MasterInterface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private final NettyRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  private volatile boolean stopped = false;
  private QueryManager qm;
  private TajoMaster master;
  private AtomicInteger processed;
  private Sleeper sleeper;
  
  public WorkerListener(TajoConf conf, QueryManager qm, TajoMaster master) {
    String confMasterAddr = conf.getVar(ConfVars.MASTER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    this.rpcServer = NettyRpc.getProtoParamRpcServer(this, 
        MasterInterface.class, initIsa);
    this.qm = qm;
    this.stopped = false;
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();
    processed = new AtomicInteger(0);
    sleeper = new Sleeper();
    this.master = master;
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }
  
  public boolean isStopped() {
    return this.stopped;
  }
  
  public void shutdown() {
    this.stopped = true;
  }

  /* (non-Javadoc)
   * @see nta.engine.ipc.AsyncMasterInterface#reportQueryUnit(nta.engine.QueryUnitProtos.QueryUnitReportProto)
   */
  @Override
  public PingResponseProto reportQueryUnit(PingRequestProto proto) {
//    LOG.info("master received reports from " + proto.getServerName() +
//        ": " + proto.getStatusCount());
    if (master.getClusterManager().getFailedWorkers().contains(proto.getServerName())) {
      LOG.info("**** Dead man alive!!!!!!");
    }

    PingRequest report = new PingRequestImpl(proto);
    for (InProgressStatusProto status : report.getProgressList()) {
      QueryUnitAttemptId uid = new QueryUnitAttemptId(status.getId());
      //qm.updateProgress(uid, status);
      QueryUnitAttempt attempt = qm.getQueryUnitAttempt(uid);
      attempt.updateProgress(status);
      processed.incrementAndGet();
    }
    PingResponseProto.Builder response
      = PingResponseProto.newBuilder();
    return response.build();
  }

  @Override
  public void run() {
    // rpc listen
    try {
      while (!this.stopped) {
        processed.set(0);
        sleeper.sleep(1000);
        LOG.info("processed: " + processed);
      }
    } catch (InterruptedException e) {
      LOG.error(ExceptionUtils.getFullStackTrace(e));
    } finally {
      rpcServer.shutdown();
    }
  }
}
