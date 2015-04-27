/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.exprrewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.plan.LogicalPlanner;
import org.apache.tajo.plan.annotator.Prioritized;
import org.apache.tajo.plan.expr.EvalContext;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.util.ClassUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class EvalTreeOptimizer {
  private static final Log LOG = LogFactory.getLog(EvalTreeOptimizer.class);

  private List<EvalTreeOptimizationRule> rules = Lists.newArrayList();

  public EvalTreeOptimizer() {
    Set<Class> functionClasses = ClassUtil.findClasses(EvalTreeOptimizationRule.class,
        EvalTreeOptimizationRule.class.getPackage().getName() + ".rules");

    for (Class eachRule : functionClasses) {
      if (!EvalTreeOptimizationRule.class.isAssignableFrom(eachRule)) {
        continue;
      }

      EvalTreeOptimizationRule rule = null;
      try {
        rule = (EvalTreeOptimizationRule)eachRule.newInstance();
      } catch (Exception e) {
        LOG.warn(eachRule + " cannot instantiate EvalTreeOptimizerRule class because of " + e.getMessage(), e);
        continue;
      }
      rules.add(rule);
    }

    Collections.sort(rules, new Comparator<EvalTreeOptimizationRule>() {
      @Override
      public int compare(EvalTreeOptimizationRule o1, EvalTreeOptimizationRule o2) {
        int priority1 = o1.getClass().getAnnotation(Prioritized.class).priority();
        int priority2 = o2.getClass().getAnnotation(Prioritized.class).priority();
        return priority1 - priority2;
      }
    });
  }

  public EvalNode optimize(LogicalPlanner.PlanContext context, EvalNode node) {
    Preconditions.checkNotNull(node);

    EvalNode optimized = node;
    for (EvalTreeOptimizationRule rule : rules) {
      optimized = rule.optimize(context, optimized);
    }

    return optimized;
  }
}
