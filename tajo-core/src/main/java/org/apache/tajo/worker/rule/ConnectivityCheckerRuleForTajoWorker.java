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

package org.apache.tajo.worker.rule;

import java.net.InetSocketAddress;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationResult;
import org.apache.tajo.rule.SelfDiagnosisRuleDefinition;
import org.apache.tajo.rule.SelfDiagnosisRuleVisibility;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRule;
import org.apache.tajo.util.HAServiceUtil;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.worker.TajoWorker;

/**
 * With this rule, Tajo worker will check the connectivity to tajo master server.
 */
@SelfDiagnosisRuleDefinition(
    category="worker", name="ConnectivityCheckerRuleForTajoWorker", priority=0, enabled = false)
@SelfDiagnosisRuleVisibility.LimitedPrivate(acceptedCallers = { TajoWorker.class })
public class ConnectivityCheckerRuleForTajoWorker implements SelfDiagnosisRule {
  
  private void checkTajoMasterConnectivity(TajoConf tajoConf) throws Exception {
    RpcConnectionPool pool = RpcConnectionPool.getPool(tajoConf);
    NettyClientBase masterClient = null;
    InetSocketAddress masterAddress = null;
    
    try {
      if (tajoConf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
        masterAddress = HAServiceUtil.getMasterUmbilicalAddress(tajoConf);
      } else {
        masterAddress = NetUtils.createSocketAddr(tajoConf.getVar(ConfVars.TAJO_MASTER_UMBILICAL_RPC_ADDRESS));
      }
      masterClient = pool.getConnection(masterAddress, TajoMasterProtocol.class, true);
      
      masterClient.getStub();
    } finally {
      if (masterClient != null) {
        pool.releaseConnection(masterClient);
      }
    }
    
  }

  @Override
  public EvaluationResult evaluate(EvaluationContext context) {
    Object tajoConfObj = context.getParameter(TajoConf.class.getName());
    EvaluationResult result = new EvaluationResult();
    
    if (tajoConfObj != null && tajoConfObj instanceof TajoConf) {
      TajoConf tajoConf = (TajoConf) tajoConfObj;
      try {
        checkTajoMasterConnectivity(tajoConf);
        
        result.setReturnCode(EvaluationResultCode.OK);
      } catch (Exception e) {
        result.setReturnCode(EvaluationResultCode.ERROR);
        result.setMessage(e.getMessage());
        result.setThrowable(e);
      }
    } else {
      result.setReturnCode(EvaluationResultCode.ERROR);
      result.setMessage("WorkerContext is null or not a WorkerContext type.");
    }
    
    return result;
  }

}
