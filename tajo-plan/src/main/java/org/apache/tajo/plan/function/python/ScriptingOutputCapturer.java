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

package org.apache.tajo.plan.function.python;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.util.TUtil;

import java.io.*;
import java.util.Map;
import java.util.UUID;

public class ScriptingOutputCapturer {
  private static Log log = LogFactory.getLog(ScriptingOutputCapturer.class);

  private static Map<String, String> outputFileNames = TUtil.newHashMap();
  //Unique ID for this run to ensure udf output files aren't corrupted from previous runs.
  private static String runId = UUID.randomUUID().toString();

  //Illustrate will set the static flag telling udf to start capturing its output.  It's up to each
  //instance to react to it and set its own flag.
  private static boolean captureOutput = false;
  private boolean instancedCapturingOutput = false;

  private FunctionDesc functionDesc;
  private OverridableConf queryContext;

  public ScriptingOutputCapturer(OverridableConf queryContext, FunctionDesc functionDesc) {
    this.queryContext = queryContext;
    this.functionDesc = functionDesc;
  }

  public static void startCapturingOutput() {
    ScriptingOutputCapturer.captureOutput = true;
  }

  public static Map<String, String> getUdfOutput() throws IOException {
    Map<String, String> udfFuncNameToOutput = TUtil.newHashMap();
    for (Map.Entry<String, String> funcToOutputFileName : outputFileNames.entrySet()) {
      StringBuffer udfOutput = new StringBuffer();
      FileInputStream fis = new FileInputStream(funcToOutputFileName.getValue());
      Reader fr = new InputStreamReader(fis, Charsets.UTF_8);
      BufferedReader br = new BufferedReader(fr);

      try {
        String line = br.readLine();
        while (line != null) {
          udfOutput.append("\t" + line + "\n");
          line = br.readLine();
        }
      } finally {
        br.close();
      }
      udfFuncNameToOutput.put(funcToOutputFileName.getKey(), udfOutput.toString());
    }
    return udfFuncNameToOutput;
  }

  public void registerOutputLocation(String functionName, String fileName) {
    outputFileNames.put(functionName, fileName);
  }

  public static String getRunId() {
    return runId;
  }

  public static boolean isClassCapturingOutput() {
    return ScriptingOutputCapturer.captureOutput;
  }

  public boolean isInstanceCapturingOutput() {
    return this.instancedCapturingOutput;
  }

  public void setInstanceCapturingOutput(boolean instanceCapturingOutput) {
    this.instancedCapturingOutput = instanceCapturingOutput;
  }
}
