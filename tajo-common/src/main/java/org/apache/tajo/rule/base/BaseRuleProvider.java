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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.rule.RuleProvider;
import org.apache.tajo.rule.RuntimeRule;
import org.apache.tajo.util.ClassUtil;

public class BaseRuleProvider implements RuleProvider {
  
  private Log LOG = LogFactory.getLog(getClass());

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public List<RuntimeRule> getDefinedRules() {
    Set<Class> classSet = ClassUtil.findClasses(RuntimeRule.class, 
        getClass().getPackage().getName());
    List<RuntimeRule> ruleList = new ArrayList<RuntimeRule>(classSet.size());
    
    for (Class<RuntimeRule> ruleClazz: classSet) {
      try {
        ruleList.add(ruleClazz.newInstance());
      } catch (Exception e) {
        LOG.warn("Cannot instantiate " + ruleClazz.getName() + " class.");
        continue;
      }
    }
    return ruleList;
  }

}
