/*
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

package org.apache.tajo.jitvec;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.plan.LogicalOptimizer;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.verifier.LogicalPlanVerifier;
import org.apache.tajo.plan.verifier.PreLogicalPlanVerifier;
import org.apache.tajo.plan.verifier.VerificationState;
import org.apache.tajo.plan.verifier.VerifyException;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.util.NetUtils;

import java.net.InetSocketAddress;

public class JitVecTestServer extends AbstractService {
  private static final Log LOG = LogFactory.getLog(JitVecTestServer.class);

  TajoTestingCluster testingCluster;

  private TajoConf conf;
  private SQLAnalyzer analyzer;
  private CatalogService catalog;
  private PreLogicalPlanVerifier preVerifier;
  private LogicalPlanner planner;
  private LogicalOptimizer optimizer;
  private LogicalPlanVerifier annotatedPlanVerifier;

  private BlockingRpcServer rpcServer;
  private JitVecTestServerProtocol.JitVecTestServerProtocolService.BlockingInterface handler;

  public JitVecTestServer() {
    super(JitVecTestServer.class.getSimpleName());
    testingCluster = TpchTestBase.getInstance().getTestingCluster();
    this.conf = testingCluster.getConfiguration();
    catalog = testingCluster.getMaster().getCatalog();

    analyzer = new SQLAnalyzer();
    preVerifier = new PreLogicalPlanVerifier(catalog);
    planner = new LogicalPlanner(catalog);
    optimizer = new LogicalOptimizer(conf);
    annotatedPlanVerifier = new LogicalPlanVerifier(conf, catalog);

    handler = new ProtocolHandler();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    rpcServer = new BlockingRpcServer(JitVecTestServerProtocol.class, handler,
        new InetSocketAddress("0.0.0.0", 30060), 1);

    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    rpcServer.start();

    super.serviceStart();

    InetSocketAddress bindAddr = rpcServer.getListenAddress();
    System.out.println("\n=============================================================================");
    System.out.println("Test Server Addr: " + NetUtils.normalizeInetSocketAddress(bindAddr));
    System.out.println("HDFS Namenode Addr: " + testingCluster.getMiniDFSCluster().getFileSystem().getUri());
    System.out.println("Catalog Server Addr: " +
        NetUtils.normalizeInetSocketAddress(testingCluster.getMaster().getCatalogServer().getBindAddress()));
    System.out.println("=============================================================================\n");
  }

  public void serviceStop() throws Exception {
    rpcServer.shutdown();

    super.serviceStop();
  }

  public class ProtocolHandler implements JitVecTestServerProtocol.JitVecTestServerProtocolService.BlockingInterface {
    @Override
    public JitVecTestServerProtocol.PlanResponse requestPlan(RpcController controller,
        JitVecTestServerProtocol.RequestPlan request) throws ServiceException {

      JitVecTestServerProtocol.PlanResponse.Builder builder = JitVecTestServerProtocol.PlanResponse.newBuilder();

      Session session = new Session("00", "tajo", "default");
      QueryContext queryContext = new QueryContext(conf, session);

      try {
        LOG.info("Request is received: " + request.getSql());
        Expr expr = analyzer.parse(request.getSql());
        VerificationState state = new VerificationState();
        preVerifier.verify(queryContext, state, expr);
        if (!state.verified()) {
          StringBuilder sb = new StringBuilder();
          for (String error : state.getErrorMessages()) {
            sb.append(error).append("\n");
          }
          throw new VerifyException(sb.toString());
        }

        LogicalPlan plan = planner.createPlan(queryContext, expr);
        if (LOG.isDebugEnabled()) {
          LOG.debug("=============================================");
          LOG.debug("Non Optimized Query: \n" + plan.toString());
          LOG.debug("=============================================");
        }
        LOG.info("Non Optimized Query: \n" + plan.toString());
        optimizer.optimize(queryContext, plan);
        LOG.info("=============================================");
        LOG.info("Optimized Query: \n" + plan.toString());
        LOG.info("=============================================");

        annotatedPlanVerifier.verify(queryContext, state, plan);

        if (!state.verified()) {
          StringBuilder sb = new StringBuilder();
          for (String error : state.getErrorMessages()) {
            sb.append(error).append("\n");
          }
          throw new VerifyException(sb.toString());
        }

        builder.setSerializedPlan(plan.getRootBlock().getRoot().toJson());

        System.out.println("\n\n======================================================================\n\n");
        System.out.println(CoreGsonHelper.getPrettyInstance().toJson(plan.getRootBlock().getRoot()));
        System.out.println("\n\n======================================================================\n\n");
      } catch (PlanningException e) {
        if (e.getMessage() != null) {
          LOG.error(e.getMessage());
          builder.setErrorMessage(e.getMessage());
        } else {
          e.printStackTrace();
          builder.setErrorMessage("Internal Error");
        }
      }

      return builder.build();
    }
  }

  public static void startServer(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    JitVecTestServer server = new JitVecTestServer();
    server.init(conf);
    server.start();
  }

  public static void main(String [] args) throws Exception {
    startServer(args);
  }
}
