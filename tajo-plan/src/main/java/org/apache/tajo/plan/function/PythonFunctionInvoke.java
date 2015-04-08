/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.plan.function;

import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.plan.function.python.PythonScriptExecutor;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;

/**
 * This class invokes the python functions.
 */
public class PythonFunctionInvoke extends FunctionInvoke implements Cloneable {

  private PythonScriptExecutor scriptExecutor;
  private FunctionInvokeContext context;

  public PythonFunctionInvoke() {

  }

  public PythonFunctionInvoke(FunctionDesc functionDesc) {
    super(functionDesc);
  }

  @Override
  public void init(FunctionInvokeContext context) throws IOException {
//    this.scriptExecutor = new PythonScriptExecutor(functionDesc);
    this.context = context;
  }

  @Override
  public Datum eval(Tuple tuple) {
    // TODO: Currently, the script executor is started and stopped for every eval() call.
    // TODO: Since it internally forks a child process which executes python functions,
    // TODO: frequent calls of start/stop functions will incur a large overhead.
    // TODO: To avoid this problem, PythonScriptExecutor should have the same life cycle with the TaskRunner.
    // TODO: In addition, we should consider the resource management problem, too.
    if (scriptExecutor == null) {
      this.scriptExecutor = new PythonScriptExecutor(functionDesc);
      try {
        scriptExecutor.start(context);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    Datum res = scriptExecutor.eval(tuple);
//    try {
//      scriptExecutor.stop();
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }
    return res;
  }
}
