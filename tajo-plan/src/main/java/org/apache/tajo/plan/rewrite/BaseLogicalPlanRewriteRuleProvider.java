/*
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

package org.apache.tajo.plan.rewrite;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.rewrite.rules.FilterPushDownRule;
import org.apache.tajo.plan.rewrite.rules.PartitionedTableRewriter;
import org.apache.tajo.plan.rewrite.rules.ProjectionPushDownRule;
import org.apache.tajo.util.TUtil;

import java.util.Collection;
import java.util.List;

/**
 * Default RewriteRuleProvider
 */
@SuppressWarnings("unused")
public class BaseLogicalPlanRewriteRuleProvider extends LogicalPlanRewriteRuleProvider {

  public BaseLogicalPlanRewriteRuleProvider(TajoConf conf) {
    super(conf);
  }

  @Override
  public Collection<Class<? extends LogicalPlanRewriteRule>> getPreRules() {
    List<Class<? extends LogicalPlanRewriteRule>> rules = TUtil.newList();

    if (systemConf.getBoolVar(TajoConf.ConfVars.$TEST_FILTER_PUSHDOWN_ENABLED)) {
      rules.add(FilterPushDownRule.class);
    }

    return rules;
  }

  @Override
  public Collection<Class<? extends LogicalPlanRewriteRule>> getPostRules() {
    List<Class<? extends LogicalPlanRewriteRule>> rules = TUtil.newList(
        ProjectionPushDownRule.class,
        PartitionedTableRewriter.class
    );
    return rules;
  }
}
