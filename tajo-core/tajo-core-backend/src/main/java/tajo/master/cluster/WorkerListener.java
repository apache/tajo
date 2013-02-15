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

package tajo.master.cluster;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.service.AbstractService;
import tajo.QueryUnitAttemptId;
import tajo.TajoIdProtos.QueryUnitAttemptIdProto;
import tajo.conf.TajoConf;
import tajo.conf.TajoConf.ConfVars;
import tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import tajo.engine.MasterWorkerProtos.TaskCompletionReport;
import tajo.engine.MasterWorkerProtos.TaskFatalErrorReport;
import tajo.engine.MasterWorkerProtos.TaskStatusProto;
import tajo.ipc.MasterWorkerProtocol;
import tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import tajo.master.TajoMaster.MasterContext;
import tajo.master.event.TaskAttemptStatusUpdateEvent;
import tajo.master.event.TaskCompletionEvent;
import tajo.master.event.TaskFatalErrorEvent;
import tajo.rpc.ProtoAsyncRpcServer;
import tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;

import java.net.InetSocketAddress;

public class WorkerListener extends AbstractService
    implements MasterWorkerProtocolService.Interface {
  
  private final static Log LOG = LogFactory.getLog(WorkerListener.class);
  private MasterContext context;
  private ProtoAsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  
  public WorkerListener(final MasterContext context) throws Exception {
    super(WorkerListener.class.getName());
    this.context = context;

    String confMasterAddr = context.getConf().getVar(ConfVars.TASKRUNNER_LISTENER_ADDRESS);
    InetSocketAddress initIsa = NetUtils.createSocketAddr(confMasterAddr);
    if (initIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initIsa);
    }
    try {
      this.rpcServer = new ProtoAsyncRpcServer(MasterWorkerProtocol.class,
          this, initIsa);
    } catch (Exception e) {
      LOG.error(e);
    }
    this.rpcServer.start();
    this.bindAddr = rpcServer.getBindAddress();
    this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

    // Setup RPC server
    // Get the master address
    LOG.info(WorkerListener.class.getSimpleName() + " is bind to " + addr);
    context.getConf().setVar(TajoConf.ConfVars.TASKRUNNER_LISTENER_ADDRESS, addr);
  }

  @Override
  public void init(Configuration conf) {
    super.init(conf);
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public void stop() {
    rpcServer.shutdown();
    super.stop();
  }
  
  public InetSocketAddress getBindAddress() {
    return this.bindAddr;
  }
  
  public String getAddress() {
    return this.addr;
  }

  static BoolProto TRUE_PROTO = BoolProto.newBuilder().setValue(true).build();

  @Override
  public void getTask(RpcController controller, ContainerIdProto request,
                      RpcCallback<QueryUnitRequestProto> done) {
    //LOG.info("Get TaskRequest from " + request.getHost());
    //context.getEventHandler().handle(new TaskRequestEvent(new NodeIdPBImpl(request), done));
  }

  @Override
  public void statusUpdate(RpcController controller, TaskStatusProto request,
                           RpcCallback<BoolProto> done) {
    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(request.getId());
    context.getEventHandler().handle(new TaskAttemptStatusUpdateEvent(attemptId,
        request));
    done.run(TRUE_PROTO);
  }

  @Override
  public void ping(RpcController controller,
                   QueryUnitAttemptIdProto attemptIdProto,
                   RpcCallback<BoolProto> done) {
    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(attemptIdProto);
    context.getQuery(attemptId.getQueryId()).getContext().getQuery().getSubQuery(attemptId.getSubQueryId()).
        getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId).
        resetExpireTime();
    done.run(TRUE_PROTO);
  }

  @Override
  public void fatalError(RpcController controller, TaskFatalErrorReport report,
                         RpcCallback<BoolProto> done) {
    context.getEventHandler().handle(new TaskFatalErrorEvent(report));
    done.run(TRUE_PROTO);
  }

  @Override
  public void done(RpcController controller, TaskCompletionReport report,
                       RpcCallback<BoolProto> done) {
    context.getEventHandler().handle(new TaskCompletionEvent(report));
    done.run(TRUE_PROTO);
  }
}
