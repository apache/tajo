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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoIdProtos.QueryUnitAttemptIdProto;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.MasterWorkerProtos.QueryUnitRequestProto;
import org.apache.tajo.engine.MasterWorkerProtos.TaskCompletionReport;
import org.apache.tajo.engine.MasterWorkerProtos.TaskFatalErrorReport;
import org.apache.tajo.engine.MasterWorkerProtos.TaskStatusProto;
import org.apache.tajo.ipc.MasterWorkerProtocol;
import org.apache.tajo.ipc.MasterWorkerProtocol.MasterWorkerProtocolService;
import org.apache.tajo.master.QueryMaster.QueryContext;
import org.apache.tajo.master.event.TaskAttemptStatusUpdateEvent;
import org.apache.tajo.master.event.TaskCompletionEvent;
import org.apache.tajo.master.event.TaskFatalErrorEvent;
import org.apache.tajo.master.event.TaskRequestEvent;
import org.apache.tajo.rpc.ProtoAsyncRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class TaskRunnerListener extends AbstractService
    implements MasterWorkerProtocolService.Interface {
  
  private final static Log LOG = LogFactory.getLog(
      org.apache.tajo.master.cluster.WorkerListener.class);
  private QueryContext context;
  private ProtoAsyncRpcServer rpcServer;
  private InetSocketAddress bindAddr;
  private String addr;
  
  public TaskRunnerListener(final QueryContext context) throws Exception {
    super(org.apache.tajo.master.cluster.WorkerListener.class.getName());
    this.context = context;


    InetSocketAddress initIsa =
        new InetSocketAddress(InetAddress.getLocalHost(), 0);
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
  }

  @Override
  public void init(Configuration conf) {
    // Setup RPC server
    try {
      InetSocketAddress initIsa =
          new InetSocketAddress(InetAddress.getLocalHost(), 0);
      if (initIsa.getAddress() == null) {
        throw new IllegalArgumentException("Failed resolve of " + initIsa);
      }

      this.rpcServer = new ProtoAsyncRpcServer(MasterWorkerProtocol.class,
          this, initIsa);

      this.rpcServer.start();
      this.bindAddr = rpcServer.getBindAddress();
      this.addr = bindAddr.getHostName() + ":" + bindAddr.getPort();

    } catch (Exception e) {
      LOG.error(e);
    }

    // Get the master address
    LOG.info(org.apache.tajo.master.cluster.WorkerListener.class.getSimpleName() + " is bind to " + addr);
    context.getConf().setVar(TajoConf.ConfVars.TASKRUNNER_LISTENER_ADDRESS, addr);

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
    context.getEventHandler().handle(new TaskRequestEvent(
        new ContainerIdPBImpl(request), done));
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
    // TODO - to be completed
    QueryUnitAttemptId attemptId = new QueryUnitAttemptId(attemptIdProto);
//    context.getQuery(attemptId.getQueryId()).getSubQuery(attemptId.getSubQueryId()).
//        getQueryUnit(attemptId.getQueryUnitId()).getAttempt(attemptId).
//        resetExpireTime();
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
