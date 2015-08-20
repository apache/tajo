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

package org.apache.tajo.plan.joinorder;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.plan.LogicalPlan;

/**
 * An interface for join order algorithms
 */
@InterfaceStability.Evolving
public interface JoinOrderAlgorithm {

  /**
   * Find the best join order.
   *
   * @param plan
   * @param block
   * @param joinGraphContext A left-deep join tree represents join conditions and the join relationships among
   *                         relations. A vertex can be a relation or a group of joined relations.
   *                         An edge represents a join relation between two vertexes.
   * @return found join order
   */
  FoundJoinOrder findBestOrder(LogicalPlan plan, LogicalPlan.QueryBlock block, JoinGraphContext joinGraphContext)
      throws TajoException;
}
