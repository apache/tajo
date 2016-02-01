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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionDescProto;

import java.util.ArrayList;
import java.util.List;

public class OutputCommitHandle {

  private List<Path> backupPaths;
  private List<Path> targetPaths;
  private List<PartitionDescProto> partitions;

  public OutputCommitHandle() {
    backupPaths = new ArrayList<Path>();
    targetPaths = new ArrayList<Path>();
    partitions = new ArrayList<PartitionDescProto>();
  }

  public List<Path> getBackupPaths() {
    return backupPaths;
  }

  public void setBackupPaths(List<Path> backupPaths) {
    this.backupPaths = backupPaths;
  }

  public void addBackupPath(Path path) {
    this.backupPaths.add(path);
  }

  public List<Path> getTargetPaths() {
    return targetPaths;
  }

  public void setTargetPaths(List<Path> renamedPaths) {
    this.targetPaths = renamedPaths;
  }

  public void addTargetPath(Path path) {
    this.targetPaths.add(path);
  }

  public List<PartitionDescProto> getPartitions() {
    return partitions;
  }

  public void setPartitions(List<PartitionDescProto> partitions) {
    this.partitions = partitions;
  }

  public void addPartition(PartitionDescProto partition) {
    this.partitions.add(partition);
  }
}
