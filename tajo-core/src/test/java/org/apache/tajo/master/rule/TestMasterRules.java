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

package org.apache.tajo.master.rule;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationFailedException;
import org.apache.tajo.rule.EvaluationResult;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRuleEngine;
import org.apache.tajo.rule.SelfDiagnosisRuleSession;
import org.apache.tajo.rule.base.TajoConfValidationRule;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMasterRules {
  
  private static Path rootFilePath;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    rootFilePath = CommonTestingUtil.getTestDir();
  }
  
  @AfterClass
  public static void tearDownClass() throws Exception {
    CommonTestingUtil.cleanupTestDir(rootFilePath.toUri().getPath());
  }

  @Test
  public void testTajoConfValidationRule() throws Exception {
    TajoConf tajoConf = new TajoConf(new YarnConfiguration());
    
    EvaluationContext context = new EvaluationContext();
    context.addParameter(TajoConf.class.getName(), tajoConf);
    
    TajoConfValidationRule validationRule = new TajoConfValidationRule();
    EvaluationResult result = validationRule.evaluate(context);
    
    assertThat(result, is(notNullValue()));
    assertThat(result.getReturnCode(), is(EvaluationResultCode.OK));
  }
  
  @Test(expected=EvaluationFailedException.class)
  public void testTajoConfValidationRuleWithException() throws Exception {
    TajoConf tajoConf = new TajoConf(new YarnConfiguration());
    SelfDiagnosisRuleEngine ruleEngine = SelfDiagnosisRuleEngine.getInstance();
    SelfDiagnosisRuleSession ruleSession = ruleEngine.newRuleSession();
    
    tajoConf.setVar(TajoConf.ConfVars.ROOT_DIR, "invalid path.");
    
    EvaluationContext context = new EvaluationContext();
    context.addParameter(TajoConf.class.getName(), tajoConf);
    
    ruleSession.withRuleNames("TajoConfValidationRule").fireRules(context);
    
    fail("EvaluationFailedException exception is expected, but it does not happen.");
  }
  
  protected void createTajoDirectories(TajoConf tajoConf) throws Exception {
    Path tajoRootDir = new Path(rootFilePath, "tajo-root");
    FileSystem rootFs = tajoRootDir.getFileSystem(tajoConf);
    FsPermission defaultPermission = FsPermission.createImmutable((short)0700);
    
    if (!rootFs.exists(tajoRootDir)) {
      rootFs.mkdirs(tajoRootDir, new FsPermission(defaultPermission));
    }
    
    tajoConf.setVar(ConfVars.ROOT_DIR, tajoRootDir.toUri().toString());
    
    Path tajoSystemDir = new Path(tajoRootDir, TajoConstants.SYSTEM_DIR_NAME);
    if (!rootFs.exists(tajoSystemDir)) {
      rootFs.mkdirs(tajoSystemDir, new FsPermission(defaultPermission));
    }
    
    Path tajoSystemResourceDir = new Path(tajoSystemDir, TajoConstants.SYSTEM_RESOURCE_DIR_NAME);
    if (!rootFs.exists(tajoSystemResourceDir)) {
      rootFs.mkdirs(tajoSystemResourceDir, new FsPermission(defaultPermission));
    }
    
    Path tajoWarehouseDir = new Path(tajoRootDir, TajoConstants.WAREHOUSE_DIR_NAME);
    if (!rootFs.exists(tajoWarehouseDir)) {
      rootFs.mkdirs(tajoWarehouseDir, new FsPermission(defaultPermission));
    }
    
    Path tajoStagingDir = new Path(tajoRootDir, "staging");
    if (!rootFs.exists(tajoStagingDir)) {
      rootFs.mkdirs(tajoStagingDir, new FsPermission(defaultPermission));
    }
    tajoConf.setVar(ConfVars.STAGING_ROOT_DIR, tajoStagingDir.toUri().toString());
  }
  
  @Test
  public void testFileSystemRule() throws Exception {
    TajoConf tajoConf = new TajoConf(new YarnConfiguration());
    
    createTajoDirectories(tajoConf);
    
    EvaluationContext context = new EvaluationContext();
    context.addParameter(TajoConf.class.getName(), tajoConf);
    
    FileSystemRule fsRule = new FileSystemRule();
    EvaluationResult result = fsRule.evaluate(context);
    
    assertThat(result, is(notNullValue()));
    assertThat(result.getReturnCode(), is(EvaluationResultCode.OK));
  }
  
  @Test
  public void testFileSystemRuleWithError() throws Exception {
    TajoConf tajoConf = new TajoConf(new YarnConfiguration());
    
    createTajoDirectories(tajoConf);
    Path systemResourceDir = TajoConf.getSystemResourceDir(tajoConf);
    FileSystem defaultFs = systemResourceDir.getFileSystem(tajoConf);
    if (defaultFs.exists(systemResourceDir)) {
      defaultFs.delete(systemResourceDir, true);
    }
    
    EvaluationContext context = new EvaluationContext();
    context.addParameter(TajoConf.class.getName(), tajoConf);
    
    FileSystemRule fsRule = new FileSystemRule();
    EvaluationResult result = fsRule.evaluate(context);
    
    assertThat(result, is(notNullValue()));
    assertThat(result.getReturnCode(), is(EvaluationResultCode.ERROR));
  }
}
