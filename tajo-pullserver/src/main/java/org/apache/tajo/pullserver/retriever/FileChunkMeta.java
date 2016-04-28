/*
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

package org.apache.tajo.pullserver.retriever;

public class FileChunkMeta {
  private final long startOffset;
  private final long length;
  private final String ebId;
  private final String taskId;

  public FileChunkMeta(long startOffset, long length, String ebId, String taskId) {
    this.startOffset = startOffset;
    this.length = length;
    this.ebId = ebId;
    this.taskId = taskId;
  }

  public String getTaskId() {
    return taskId;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getLength() {
    return length;
  }

  public String getEbId() {
    return ebId;
  }

  public String toString() {
    return "ebId: " + ebId + ", taskId: " + taskId + " (" + startOffset + ", " + length + ")";
  }
}
