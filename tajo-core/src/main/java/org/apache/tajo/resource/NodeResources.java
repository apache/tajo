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

package org.apache.tajo.resource;


public class NodeResources {

  public static NodeResource createResource(int memory) {
    return createResource(memory, 0);
  }

  public static NodeResource createResource(int memory, int disks) {
    return NodeResource.createResource(memory, disks, (memory > 0) ? 1 : 0);
  }

  public static NodeResource createResource(int memory, int disks, int vCores) {
    return NodeResource.createResource(memory, disks, vCores);
  }

  public static NodeResource clone(NodeResource res) {
    return NodeResource.createResource(res.getMemory(), res.getDisks(), res.getVirtualCores());
  }

  public static NodeResource update(NodeResource lhs, NodeResource rhs) {
    return lhs.setMemory(rhs.getMemory()).setDisks(rhs.getDisks()).setVirtualCores(rhs.getVirtualCores());
  }

  public static NodeResource addTo(NodeResource lhs, NodeResource rhs) {
    lhs.setMemory(lhs.getMemory() + rhs.getMemory())
        .setVirtualCores(lhs.getVirtualCores() + rhs.getVirtualCores())
        .setDisks(lhs.getDisks() + rhs.getDisks());
    return lhs;
  }

  public static NodeResource add(NodeResource lhs, NodeResource rhs) {
    return addTo(clone(lhs), rhs);
  }

  public static NodeResource subtractFrom(NodeResource lhs, NodeResource rhs) {
    lhs.setMemory(lhs.getMemory() - rhs.getMemory())
        .setVirtualCores(lhs.getVirtualCores() - rhs.getVirtualCores())
        .setDisks(lhs.getDisks() - rhs.getDisks());
    return lhs;
  }

  public static NodeResource subtract(NodeResource lhs, NodeResource rhs) {
    return subtractFrom(clone(lhs), rhs);
  }

  public static NodeResource multiplyTo(NodeResource lhs, double by) {
    lhs.setMemory((int) (lhs.getMemory() * by))
        .setVirtualCores((int) (lhs.getVirtualCores() * by))
        .setDisks((int) (lhs.getDisks() * by));
    return lhs;
  }

  public static NodeResource multiply(NodeResource lhs, double by) {
    return multiplyTo(clone(lhs), by);
  }
  
  public static NodeResource multiplyAndNormalizeUp(
      ResourceCalculator calculator,NodeResource lhs, double by, NodeResource factor) {
    return calculator.multiplyAndNormalizeUp(lhs, by, factor);
  }

  public static NodeResource multiplyAndNormalizeDown(
      ResourceCalculator calculator,NodeResource lhs, double by, NodeResource factor) {
    return calculator.multiplyAndNormalizeDown(lhs, by, factor);
  }

  public static NodeResource multiplyAndRoundDown(NodeResource lhs, double by) {
    NodeResource out = clone(lhs);
    out.setMemory((int)(lhs.getMemory() * by));
    out.setDisks((int)(lhs.getDisks() * by));
    out.setVirtualCores((int)(lhs.getVirtualCores() * by));
    return out;
  }

  public static NodeResource normalize(
      ResourceCalculator calculator, NodeResource lhs, NodeResource min,
      NodeResource max, NodeResource increment) {
    return calculator.normalize(lhs, min, max, increment);
  }

  public static NodeResource roundUp(
      ResourceCalculator calculator, NodeResource lhs, NodeResource factor) {
    return calculator.roundUp(lhs, factor);
  }

  public static NodeResource roundDown(
      ResourceCalculator calculator, NodeResource lhs, NodeResource factor) {
    return calculator.roundDown(lhs, factor);
  }

  public static boolean isInvalidDivisor(
      ResourceCalculator resourceCalculator, NodeResource divisor) {
    return resourceCalculator.isInvalidDivisor(divisor);
  }

  public static float ratio(
      ResourceCalculator resourceCalculator, NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.ratio(lhs, rhs);
  }

  public static float divide(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource, NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.divide(clusterResource, lhs, rhs);
  }

  public static NodeResource divideAndCeil(
      ResourceCalculator resourceCalculator, NodeResource lhs, int rhs) {
    return resourceCalculator.divideAndCeil(lhs, rhs);
  }

  public static boolean equals(NodeResource lhs, NodeResource rhs) {
    return lhs.equals(rhs);
  }

  public static boolean lessThan(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) < 0);
  }

  public static boolean lessThanOrEqual(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return (resourceCalculator.compare(clusterResource, lhs, rhs) <= 0);
  }

  public static boolean greaterThan(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) > 0;
  }

  public static boolean greaterThanOrEqual(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0;
  }

  public static NodeResource min(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) <= 0 ? lhs : rhs;
  }

  public static NodeResource max(
      ResourceCalculator resourceCalculator,
      NodeResource clusterResource,
      NodeResource lhs, NodeResource rhs) {
    return resourceCalculator.compare(clusterResource, lhs, rhs) >= 0 ? lhs : rhs;
  }

  public static boolean fitsIn(NodeResource smaller, NodeResource bigger) {
    return smaller.getMemory() <= bigger.getMemory() &&
        smaller.getDisks() <= bigger.getDisks() &&
        smaller.getVirtualCores() <= bigger.getVirtualCores();
  }

  public static NodeResource componentwiseMin(NodeResource lhs, NodeResource rhs) {
    return createResource(Math.min(lhs.getMemory(), rhs.getMemory()),
        Math.min(lhs.getDisks(), rhs.getDisks()),
        Math.min(lhs.getVirtualCores(), rhs.getVirtualCores()));
  }

  public static NodeResource componentwiseMax(NodeResource lhs, NodeResource rhs) {
    return createResource(Math.max(lhs.getMemory(), rhs.getMemory()),
        Math.max(lhs.getDisks(), rhs.getDisks()),
        Math.max(lhs.getVirtualCores(), rhs.getVirtualCores()));
  }
}
