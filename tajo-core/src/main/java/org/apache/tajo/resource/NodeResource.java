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

import com.google.common.base.Objects;
import org.apache.tajo.TajoProtos;
import org.apache.tajo.common.ProtoObject;

/**
 * <p><code>NodeResource</code> models a set of computer resources in the
 * cluster.</p>
 * <p/>
 * <p>Currently it models  <em>memory</em> and <em>disk</em> and <em>CPU</em>.</p>
 * <p/>
 * <p>The unit for memory is megabytes. The unit for disks is the number of disk.
 * CPU is modeled with virtual cores (vcores), a unit for expressing parallelism.
 * A node's capacity should be configured with virtual cores equal to its number of physical cores.
 * A task should be requested with the number of cores it can saturate.</p>
 * <p/>
 */

public class NodeResource implements ProtoObject<TajoProtos.NodeResourceProto>, Comparable<NodeResource> {

  private int memory;
  private int disks;
  private int vCores;

  public NodeResource(TajoProtos.NodeResourceProto proto) {
    this.memory = proto.getMemory();
    this.vCores = proto.getVirtualCores();
    this.disks = proto.getDisks();
  }

  private NodeResource() {

  }

  public static NodeResource createResource(int memory,  int disks, int vCores) {
    return new NodeResource().setMemory(memory).setDisks(disks).setVirtualCores(vCores);
  }

  /**
   * Get <em>memory</em> of the resource.
   *
   * @return <em>memory</em> of the resource
   */
  public int getMemory() {
    return memory;
  }

  /**
   * Set <em>memory</em> of the resource.
   *
   * @param memory <em>memory</em> of the resource
   */
  public NodeResource setMemory(int memory) {
    this.memory = memory;
    return this;
  }


  /**
   * Get <em>number of disks</em> of the resource.
   *
   * @return <em>number of disks</em> of the resource
   */
  public int getDisks() {
    return disks;
  }

  /**
   * Set <em>number of disks </em> of the resource.
   *
   * @param disks <em>number of disks</em> of the resource
   */
  public NodeResource setDisks(int disks) {
    this.disks = disks;
    return this;
  }

  /**
   * Get <em>number of virtual cpu cores</em> of the resource.
   * Virtual cores are a unit for expressing CPU parallelism. A node's capacity
   * should be configured with virtual cores equal to its number of physical cores.
   *
   * @return <em>num of virtual cpu cores</em> of the resource
   */
  public int getVirtualCores() {
    return vCores;
  }


  /**
   * Set <em>number of virtual cpu cores</em> of the resource.
   *
   * @param vCores <em>number of virtual cpu cores</em> of the resource
   */
  public NodeResource setVirtualCores(int vCores) {
    this.vCores = vCores;
    return this;
  }

  @Override
  public TajoProtos.NodeResourceProto getProto() {
    TajoProtos.NodeResourceProto.Builder builder = TajoProtos.NodeResourceProto.newBuilder();
    builder.setMemory(memory)
        .setDisks(disks)
        .setVirtualCores(vCores);
    return builder.build();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getMemory(), getDisks(), getVirtualCores());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof NodeResource))
      return false;
    NodeResource other = (NodeResource) obj;
    if (getMemory() != other.getMemory() ||
        getDisks() != other.getDisks() ||
        getVirtualCores() != other.getVirtualCores()) {
      return false;
    }
    return true;
  }

  @Override
  public int compareTo(NodeResource other) {
    int diff = this.getMemory() - other.getMemory();
    if (diff == 0) {
      diff = this.getDisks() - other.getDisks();
    }
    if (diff == 0) {
      diff = this.getVirtualCores() - other.getVirtualCores();
    }
    return diff;
  }

  @Override
  public String toString() {
    return "<memory:" + getMemory() + ", disks:" + getDisks() + ", vCores:" + getVirtualCores() + ">";
  }
}
