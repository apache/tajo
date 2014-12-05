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

import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationResult;
import org.apache.tajo.rule.RuleDefinition;
import org.apache.tajo.rule.RuleVisibility;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.RuntimeRule;
import org.apache.tajo.worker.TajoWorker;
import org.apache.tajo.worker.TajoWorker.WorkerContext;

/**
 * With this rule, Tajo worker will check the connectivity to tajo master server.
 */
@RuleDefinition(category="worker", name="ConnectivityCheckerRuleForTajoWorker", priority=0)
@RuleVisibility.LimitedPrivate(acceptedCallers = { TajoWorker.class })
public class ConnectivityCheckerRuleForTajoWorker implements RuntimeRule {
  
  private void checkTajoMasterConnectivity(WorkerContext workerContext) throws Exception {
    RpcConnectionPool pool = RpcConnectionPool.getPool(workerContext.getConf());
    NettyClientBase masterClient = null;
    
    try {
      masterClient = pool.getConnection(workerContext.getTajoMasterAddress(), 
          TajoMasterProtocol.class, true);
      
      masterClient.getStub();
    } finally {
      if (masterClient != null) {
        pool.releaseConnection(masterClient);
      }
    }
    
  }

  @Override
  public EvaluationResult evaluate(EvaluationContext context) {
    Object workerContextObj = context.getParameter(WorkerContext.class.getName());
    EvaluationResult result = new EvaluationResult();
    
    if (workerContextObj != null && workerContextObj instanceof WorkerContext) {
      WorkerContext workerContext = (WorkerContext) workerContextObj;
      try {
        checkTajoMasterConnectivity(workerContext);
        
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
