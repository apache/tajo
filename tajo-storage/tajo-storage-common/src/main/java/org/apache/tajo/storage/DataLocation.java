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

package org.apache.tajo.storage;

public class DataLocation {
  public static final int UNSET_VOLUME_ID = Integer.MIN_VALUE;
  public static final int UNKNOWN_VOLUME_ID = -1;
  public static final int REMOTE_VOLUME_ID = -2;

  private String host;
  private int volumeId;

  public DataLocation(String host, int volumeId) {
    this.host = host;
    this.volumeId = volumeId;
  }

  public String getHost() {
    return host;
  }

  /**
   * <h3>Volume id</h3>
   * Volume id is an integer. Each volume id identifies each disk volume.
   *
   * This volume id can be obtained from org.apache.hadoop.fs.BlockStorageLocation#getVolumeIds()}.
   * HDFS cannot give any volume id due to unknown reason
   * and disabled config 'dfs.client.file-block-locations.enabled'.
   * In this case, the volume id will be -1 or other native integer.
   */
  public int getVolumeId() {
    return volumeId;
  }

  @Override
  public String toString() {
    return "DataLocation{" +
        "host=" + host +
        ", volumeId=" + volumeId +
        '}';
  }
}
