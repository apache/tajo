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

package org.apache.tajo.engine.planner.global.rewriter;

import com.google.common.collect.Lists;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.planner.global.rewriter.rules.BroadcastJoinRule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("unused")
public class BaseGlobalPlanRewriteRuleProvider extends GlobalPlanRewriteRuleProvider {
  private static final List<Class<? extends GlobalPlanRewriteRule>> EMPTY_RULES = new ArrayList<>();

  public BaseGlobalPlanRewriteRuleProvider(TajoConf conf) {
    super(conf);
  }

  @Override
  public Collection<Class<? extends GlobalPlanRewriteRule>> getRules() {
    List<Class<? extends GlobalPlanRewriteRule>> rules = Lists.newArrayList();
    rules.add(BroadcastJoinRule.class);
    return rules;
  }
}
