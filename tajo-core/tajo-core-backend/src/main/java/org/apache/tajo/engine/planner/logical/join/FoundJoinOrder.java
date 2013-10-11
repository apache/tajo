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

package org.apache.tajo.engine.planner.logical.join;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.logical.JoinNode;

/**
 * It contains the result of join enumeration.
 */
@InterfaceStability.Evolving
public class FoundJoinOrder {
  private JoinNode joinNode;
  private EvalNode[] quals;
  private double cost;

  public FoundJoinOrder(JoinNode joinNode, EvalNode[] quals, double cost) {
    this.joinNode = joinNode;
    this.quals = quals;
    this.cost = cost;
  }

  /**
   * @return a ordered join operators
   */
  public JoinNode getOrderedJoin() {
    return this.joinNode;
  }

  public double getCost() {
    return cost;
  }

  /**
   * The search conditions contained in where clause are pushed down into join operators.
   * This method returns the remain search conditions except for pushed down condition.
   *
   * @return the remain search conditions
   */
  public EvalNode [] getRemainConditions() {
    return this.quals;
  }
}
