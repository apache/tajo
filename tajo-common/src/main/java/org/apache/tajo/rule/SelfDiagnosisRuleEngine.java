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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.tajo.util.TUtil;

public class SelfDiagnosisRuleEngine {

  private final Map<String, Map<String, RuleWrapper>> wrapperMap;
  private static volatile SelfDiagnosisRuleEngine instance;
  
  private SelfDiagnosisRuleEngine() {
    wrapperMap = TUtil.newHashMap();
    loadPredefinedRules();
  }
  
  public static SelfDiagnosisRuleEngine getInstance() {
    if (instance == null) {
      synchronized (SelfDiagnosisRuleEngine.class) {
        if (instance == null) {
          instance = new SelfDiagnosisRuleEngine();
        }
      }
    }
    return instance;
  }
  
  public void reset() {
    if (wrapperMap != null) {
      wrapperMap.clear();
    }
    loadPredefinedRules();
  }
  
  public SelfDiagnosisRuleSession newRuleSession() {
    return new SelfDiagnosisRuleSession(this);
  }
  
  protected Map<String, Map<String, RuleWrapper>> getRules() {
    return wrapperMap;    
  }
  
  private void loadRuleData(List<SelfDiagnosisRule> ruleList) {
    for (SelfDiagnosisRule rule: ruleList) {
      RuleWrapper wrapper = new RuleWrapper(rule);
      if (wrapper.isEnabled()) {
        Map<String, RuleWrapper> categoryMap = wrapperMap.get(wrapper.getCategoryName());

        if (categoryMap == null) {
          categoryMap = TUtil.newHashMap();
          wrapperMap.put(wrapper.getCategoryName(), categoryMap);
        }

        categoryMap.put(wrapper.getRuleName(), wrapper);
      }
    }
  }

  protected void loadPredefinedRules() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    ServiceLoader<SelfDiagnosisRuleProvider> serviceLoader = ServiceLoader.load(SelfDiagnosisRuleProvider.class, cl);
    Iterator<SelfDiagnosisRuleProvider> iterator = serviceLoader.iterator();
    
    wrapperMap.clear();
    while (iterator.hasNext()) {
      SelfDiagnosisRuleProvider ruleProvider = iterator.next();
      loadRuleData(ruleProvider.getDefinedRules());
    }
  }
  
  static class RuleWrapper implements Comparable<RuleWrapper> {
    private final String categoryName;
    private final String ruleName;
    private final int priority;
    private final boolean enabled;
    private final Class<?>[] acceptedCallers;
    private final SelfDiagnosisRule rule;
    
    public RuleWrapper(SelfDiagnosisRule rule) {
      this.rule = rule;
      
      SelfDiagnosisRuleDefinition ruleDefinition = rule.getClass().getAnnotation(SelfDiagnosisRuleDefinition.class);
      if (ruleDefinition == null) {
        throw new IllegalArgumentException(rule.getClass().getName() + " is not a valid runtime rule.");
      }
      categoryName = ruleDefinition.category();
      ruleName = ruleDefinition.name();
      priority = ruleDefinition.priority();
      enabled = ruleDefinition.enabled();
      
      SelfDiagnosisRuleVisibility.LimitedPrivate limitedPrivateScope = 
          rule.getClass().getAnnotation(SelfDiagnosisRuleVisibility.LimitedPrivate.class);
      if (limitedPrivateScope != null) {
        acceptedCallers =
            Arrays.copyOf(limitedPrivateScope.acceptedCallers(), 
                limitedPrivateScope.acceptedCallers().length);
      } else {
        acceptedCallers = new Class<?>[0];
      }
    }

    public String getCategoryName() {
      return categoryName;
    }

    public String getRuleName() {
      return ruleName;
    }

    public Class<?>[] getAcceptedCallers() {
      return acceptedCallers;
    }

    public SelfDiagnosisRule getRule() {
      return rule;
    }
    
    public int getPriority() {
      return priority;
    }

    public boolean isEnabled() {
      return enabled;
    }

    @Override
    public int compareTo(RuleWrapper o) {
      if (getPriority() < 0 && o.getPriority() < 0) {
        return 0;
      } else if (getPriority() < 0) {
        return 1;
      } else if (o.getPriority() < 0) {
        return -1;
      }
      return (int) Math.signum(getPriority() - o.getPriority());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(acceptedCallers);
      result = prime * result + ((categoryName == null) ? 0 : categoryName.hashCode());
      result = prime * result + (enabled ? 1231 : 1237);
      result = prime * result + priority;
      result = prime * result + ((rule == null) ? 0 : rule.hashCode());
      result = prime * result + ((ruleName == null) ? 0 : ruleName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      RuleWrapper other = (RuleWrapper) obj;
      if (!Arrays.equals(acceptedCallers, other.acceptedCallers))
        return false;
      if (categoryName == null) {
        if (other.categoryName != null)
          return false;
      } else if (!categoryName.equals(other.categoryName))
        return false;
      if (enabled != other.enabled)
        return false;
      if (priority != other.priority)
        return false;
      if (rule == null) {
        if (other.rule != null)
          return false;
      } else if (!rule.equals(other.rule))
        return false;
      if (ruleName == null) {
        if (other.ruleName != null)
          return false;
      } else if (!ruleName.equals(other.ruleName))
        return false;
      return true;
    }
    
  }
  
}
