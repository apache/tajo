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

import java.util.Collection;

public abstract class LogicalPlanRewriteRuleProvider {
  protected final TajoConf systemConf;

  public LogicalPlanRewriteRuleProvider(TajoConf systemConf) {
    this.systemConf = systemConf;
  }

  /**
   * It returns RewriteRule classes which should be executed before join ordering.
   *
   * @return RewriteRule classes
   */
  public abstract Collection<Class<? extends LogicalPlanRewriteRule>> getPreRules();
  /**
   * It returns RewriteRule classes which should be executed after join ordering.
   *
   * @return RewriteRule classes
   */
  public abstract Collection<Class<? extends LogicalPlanRewriteRule>> getPostRules();
}
