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

package org.apache.tajo.rule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRuleEngine.RuleWrapper;
import org.apache.tajo.util.TUtil;

public class SelfDiagnosisRuleSession {
  
  private final SelfDiagnosisRuleEngine ruleEngine;
  private final Set<String> categoryPredicate;
  private final Set<String> rulePredicate;

  protected SelfDiagnosisRuleSession(SelfDiagnosisRuleEngine engine) {
    ruleEngine = engine;
    categoryPredicate = TUtil.newHashSet();
    rulePredicate = TUtil.newHashSet();
  }
  
  public SelfDiagnosisRuleSession withCategoryNames(String...categories) {
    categoryPredicate.addAll(Arrays.asList(categories));
    return this;
  }
  
  public SelfDiagnosisRuleSession withRuleNames(String...rules) {
    rulePredicate.addAll(Arrays.asList(rules));
    return this;
  }
  
  public SelfDiagnosisRuleSession reset() {
    categoryPredicate.clear();
    rulePredicate.clear();
    return this;
  }
  
  public void fireRules(EvaluationContext context) throws EvaluationFailedException {
    List<RuleWrapper> candidateRules = getCandidateRules();
    
    for (RuleWrapper wrapper: candidateRules) {
      EvaluationResult result = wrapper.getRule().evaluate(context);
      
      if (result.getReturnCode() == EvaluationResultCode.ERROR) {
        throw new EvaluationFailedException(result.getMessage(), result.getThrowable());
      }
    }
  }
  
  protected List<RuleWrapper> getCandidateRules() {
    Map<String, Map<String, RuleWrapper>> wrapperMap = null;
    List<RuleWrapper> candidateRules = TUtil.newList();
    
    wrapperMap = ruleEngine.getRules();
    Class<?> callerClazz = getCallerClassName();
    
    for (String categoryName: wrapperMap.keySet()) {
      if (categoryPredicate.size() > 0 && !categoryPredicate.contains(categoryName)) {
        continue;
      }
      
      Map<String, RuleWrapper> ruleMap = wrapperMap.get(categoryName);
      for (String ruleName: ruleMap.keySet()) {
        if (rulePredicate.size() > 0 && !rulePredicate.contains(ruleName)) {
          continue;
        }
        
        RuleWrapper ruleWrapper = ruleMap.get(ruleName);
        
        if (callerClazz != null && ruleWrapper.getAcceptedCallers().length > 0 
            && !hasCallerClazz(callerClazz, ruleWrapper.getAcceptedCallers())) {
          continue;
        }
        
        candidateRules.add(ruleWrapper);
      }
    }
    
    Collections.sort(candidateRules);
    return candidateRules;
  }
  
  protected boolean hasCallerClazz(Class<?> callerClazz, Class<?>[] acceptedCallers) {
    boolean result = false;
    
    String callerClazzName = callerClazz.getName();
    for (Class<?> acceptedCaller: acceptedCallers) {
      if (callerClazzName.equals(acceptedCaller.getName())) {
        result = true;
        break;
      }
    }
    
    return result;
  }
  
  protected Class<?> getCallerClassName() {
    return new RuleSessionSecurityManager().getCallerClassName();
  }
  
  class RuleSessionSecurityManager extends SecurityManager {
    public Class<?> getCallerClassName() {
      Class<?>[] clazzArray = getClassContext();
      int clazzIdx = 2;
      for (; clazzIdx < clazzArray.length; clazzIdx++) {
        if (!clazzArray[clazzIdx].getName().equals(SelfDiagnosisRuleSession.class.getName())) {
          break;
        }
      }
      return clazzIdx < clazzArray.length?clazzArray[clazzIdx]:null;
    }
  }
}
