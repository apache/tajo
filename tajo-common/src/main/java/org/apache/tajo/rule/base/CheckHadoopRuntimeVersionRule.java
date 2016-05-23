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

package org.apache.tajo.rule.base;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.rule.*;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.validation.Validators;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@SelfDiagnosisRuleDefinition(category="base", name = "CheckHadoopRuntimeVersionRule", priority = 0)
@SelfDiagnosisRuleVisibility.Public
public class CheckHadoopRuntimeVersionRule implements SelfDiagnosisRule {

  private static final Log LOG = LogFactory.getLog(CheckHadoopRuntimeVersionRule.class);
  private final Properties versionInfo;
  
  public CheckHadoopRuntimeVersionRule() {
    InputStream is = ClassLoader.getSystemResourceAsStream("common-version-info.properties");
    versionInfo = new Properties();
    try {
      versionInfo.load(is);
    } catch (IOException e) {
      LOG.warn(e.getMessage(), e);
    } finally {
      IOUtils.closeStream(is);
    }
  }
  
  private int[] getVersion() {
    int[] version = new int[0];
    String versionString = versionInfo.getProperty("version").split("-")[0];
    
    if (versionString != null && !versionString.isEmpty()) {
      Validators.patternMatch("\\d+\\.\\d+\\.\\d+.*").validate(versionString, true);
      
      String[] versionArray = versionString.split("\\.");
      version = new int[versionArray.length];
      for (int idx = 0; idx < versionArray.length; idx++) {
        version[idx] = Integer.parseInt(versionArray[idx]);
      }
    }
    
    return version;
  }
  
  private int compareVersion(int[] left, int[] right) {
    int returnValue = 0;
    int minLength = Math.min(left.length, right.length);
    
    for (int idx = 0; idx < minLength; idx++) {
      returnValue = (int) Math.signum(left[idx] - right[idx]);
      if (returnValue != 0) {
        break;
      }
    }
    
    if (returnValue == 0) {
      returnValue = (int) Math.signum(left.length - right.length);
    }
    return returnValue;
  }

  @Override
  public EvaluationResult evaluate(EvaluationContext context) {
    EvaluationResult evalResult = new EvaluationResult();
    
    try {
      int compared = compareVersion(getVersion(), new int[] {2, 3, 0});
      if (compared >= 0) {
        evalResult.setReturnCode(EvaluationResultCode.OK);
        evalResult.setMessage("Version test for hadoop common has passed.");
      } else {
        evalResult.setReturnCode(EvaluationResultCode.ERROR);
        evalResult.setMessage("Checking the version of hadoop common component has failed.\n" +
            "Current version : " + versionInfo.getProperty("version"));
      }
    } catch (Exception e) {
      evalResult.setReturnCode(EvaluationResultCode.ERROR);
      evalResult.setThrowable(e);
      evalResult.setMessage("Checking the version of hadoop common component has failed.");
    }
    
    return evalResult;
  }

}
