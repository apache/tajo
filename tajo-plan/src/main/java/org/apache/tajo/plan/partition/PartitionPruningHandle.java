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

package org.apache.tajo.plan.partition;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.plan.expr.EvalNode;

import java.util.Map;

/**
 * This includes result informs of partition pruning.
 *
 */
public class PartitionPruningHandle {
  private Path[] partitionPaths;
  private String[] partitionKeys;
  private long totalVolume;
  private Map<Path, String> partitionMap;
  private EvalNode[] conjunctiveForms;

  public PartitionPruningHandle(Path[] partitionPaths, String[] partitionKeys, long totalVolume,
                                Map<Path, String> partitionMap) {
    this.partitionPaths = partitionPaths;
    this.partitionKeys = partitionKeys;
    this.totalVolume = totalVolume;
    this.partitionMap = partitionMap;
  }

  public Path[] getPartitionPaths() {
    return partitionPaths;
  }

  public String[] getPartitionKeys() {
    return partitionKeys;
  }

  public long getTotalVolume() {
    return totalVolume;
  }

  public Map<Path, String> getPartitionMap() {
    return partitionMap;
  }

  public boolean hasConjunctiveForms() {
    return this.conjunctiveForms != null;
  }

  public void setConjunctiveForms(EvalNode[] conjunctiveForms) {
    this.conjunctiveForms = conjunctiveForms;
  }
}