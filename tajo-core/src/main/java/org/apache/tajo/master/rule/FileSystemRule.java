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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationResult;
import org.apache.tajo.rule.SelfDiagnosisRuleDefinition;
import org.apache.tajo.rule.SelfDiagnosisRuleVisibility;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRule;

@SelfDiagnosisRuleDefinition(category="master", name="FileSystemRule")
@SelfDiagnosisRuleVisibility.LimitedPrivate(acceptedCallers = { TajoMaster.class })
public class FileSystemRule implements SelfDiagnosisRule {
  
  private void canAccessToPath(FileStatus fsStatus, FsAction action) throws Exception {
    FsPermission permission = fsStatus.getPermission();
    UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
    String userName = userGroupInformation.getShortUserName();
    List<String> groupList = Arrays.asList(userGroupInformation.getGroupNames());
    
    if (userName.equals(fsStatus.getOwner())) {
      if (permission.getUserAction().implies(action)) {
        return;
      }
    } else if (groupList.contains(fsStatus.getGroup())) {
      if (permission.getGroupAction().implies(action)) {
        return;
      }
    } else {
      if (permission.getOtherAction().implies(action)) {
        return;
      }
    }
    throw new AccessControlException(String.format(
        "Permission denied: user=%s, path=\"%s\":%s:%s:%s%s", userName, fsStatus.getPath(),
        fsStatus.getOwner(), fsStatus.getGroup(), fsStatus.isDirectory() ? "d" : "-", permission));
  }
  
  private void checkAccessControlOnTajoPaths(TajoConf tajoConf) throws Exception {
    Path tajoRootPath = TajoConf.getTajoRootDir(tajoConf);
    FileSystem defaultFs = tajoRootPath.getFileSystem(tajoConf);    
    canAccessToPath(defaultFs.getFileStatus(tajoRootPath), FsAction.READ_WRITE);
    
    Path systemPath = TajoConf.getSystemDir(tajoConf);    
    canAccessToPath(defaultFs.getFileStatus(systemPath), FsAction.READ_WRITE);
    
    Path systemResourcePath = TajoConf.getSystemResourceDir(tajoConf);
    canAccessToPath(defaultFs.getFileStatus(systemResourcePath), FsAction.READ_WRITE);
    
    Path wareHousePath = TajoConf.getWarehouseDir(tajoConf);
    canAccessToPath(defaultFs.getFileStatus(wareHousePath), FsAction.READ_WRITE);
    
    Path stagingPath = TajoConf.getDefaultRootStagingDir(tajoConf);
    canAccessToPath(defaultFs.getFileStatus(stagingPath), FsAction.READ_WRITE);
  }

  @Override
  public EvaluationResult evaluate(EvaluationContext context) {
    EvaluationResult result = new EvaluationResult();
    Object tajoConfObj = context.getParameter(TajoConf.class.getName());
    
    if (tajoConfObj != null && tajoConfObj instanceof TajoConf) {
      TajoConf tajoConf = (TajoConf) tajoConfObj;
      try {
        checkAccessControlOnTajoPaths(tajoConf);
        
        result.setReturnCode(EvaluationResultCode.OK);
      } catch (Exception e) {
        result.setReturnCode(EvaluationResultCode.ERROR);
        result.setMessage("Current User cannot access to this filesystem.");
        result.setThrowable(e);
      }
    } else {
      result.setReturnCode(EvaluationResultCode.ERROR);
      result.setMessage("Tajo Configuration is null or not a Configuration Type.");
    }
    
    return result;
  }

}
