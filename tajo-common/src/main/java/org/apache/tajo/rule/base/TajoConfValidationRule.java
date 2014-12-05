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

import java.util.Collection;
import java.util.Set;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.rule.EvaluationContext;
import org.apache.tajo.rule.EvaluationResult;
import org.apache.tajo.rule.SelfDiagnosisRuleVisibility;
import org.apache.tajo.rule.EvaluationResult.EvaluationResultCode;
import org.apache.tajo.rule.SelfDiagnosisRuleDefinition;
import org.apache.tajo.rule.SelfDiagnosisRule;
import org.apache.tajo.util.TUtil;
import org.apache.tajo.validation.ConstraintViolation;
import org.apache.tajo.validation.ConstraintViolationException;
import org.apache.tajo.validation.Validator;

@SelfDiagnosisRuleDefinition(category="base", name="TajoConfValidationRule", priority=0)
@SelfDiagnosisRuleVisibility.Public
public class TajoConfValidationRule implements SelfDiagnosisRule {
  
  private Collection<ConstraintViolation> isValidationTestPassed(TajoConf.ConfVars confVar, String varValue) {
    Set<ConstraintViolation> violationSet = TUtil.newHashSet();
    
    if (varValue != null && confVar.valueClass() != null && confVar.validator() != null) {
      Class<?> valueClazz = confVar.valueClass();
      Validator validator = confVar.validator();
      
      if (Integer.class.getName().equals(valueClazz.getName())) {
        int intValue = Integer.parseInt(varValue);
        violationSet.addAll(validator.validate(intValue));
      } else if (Long.class.getName().equals(valueClazz.getName())) {
        long longValue = Long.parseLong(varValue);
        violationSet.addAll(validator.validate(longValue));
      } else if (Float.class.getName().equals(valueClazz.getName())) {
        float floatValue = Float.parseFloat(varValue);
        violationSet.addAll(validator.validate(floatValue));
      } else {
        violationSet.addAll(validator.validate(varValue));
      }
    }
    
    return violationSet;
  }

  @Override
  public EvaluationResult evaluate(EvaluationContext context) {
    EvaluationResult result = new EvaluationResult();
    Object tajoConfObj = context.getParameter(TajoConf.class.getName());
    result.setReturnCode(EvaluationResultCode.OK);
    
    if (tajoConfObj != null && tajoConfObj instanceof TajoConf) {
      TajoConf tajoConf = (TajoConf) tajoConfObj;
      
      for (TajoConf.ConfVars confVar: TajoConf.ConfVars.values()) {
        String varValue = tajoConf.get(confVar.keyname());
        Collection<ConstraintViolation> violationSet = isValidationTestPassed(confVar, varValue);
        
        if (violationSet.size() > 0) {
          result.setReturnCode(EvaluationResultCode.ERROR);
          result.setMessage("Validation Test has been failed on " + confVar.keyname() + 
              ". Actual value is " + varValue);
          result.setThrowable(new ConstraintViolationException(violationSet));
          break;
        }
      }
    }
    
    return result;
  }

}
