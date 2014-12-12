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

import com.google.common.base.Objects;

public class DiskMountInfo implements Comparable<DiskMountInfo> {
	private String mountPath;
	
	private long capacity;
	private long used;
	
	private int deviceId;
	
	public DiskMountInfo(int deviceId, String mountPath) {
		this.mountPath = mountPath;
	}

	public String getMountPath() {
		return mountPath;
	}

	public void setMountPath(String mountPath) {
		this.mountPath = mountPath;
	}

	public long getCapacity() {
		return capacity;
	}

	public void setCapacity(long capacity) {
		this.capacity = capacity;
	}

	public long getUsed() {
		return used;
	}

	public void setUsed(long used) {
		this.used = used;
	}

	public int getDeviceId() {
		return deviceId;
	}

  @Override
  public boolean equals(Object obj){
    if (!(obj instanceof DiskMountInfo)) return false;

    if (compareTo((DiskMountInfo) obj) == 0) return true;
    else return false;
  }

  @Override
  public int hashCode(){
    return Objects.hashCode(mountPath);
  }

	@Override
	public int compareTo(DiskMountInfo other) {
		String path1 = mountPath;
		String path2 = other.mountPath;
		
		int path1Depth = "/".equals(path1) ? 0 : path1.split("/", -1).length - 1 ;
		int path2Depth = "/".equals(path2) ? 0 : path2.split("/", -1).length - 1 ;
		
		if(path1Depth > path2Depth) {
			return -1;
		} else if(path1Depth < path2Depth) {
			return 1;
		} else {
			int path1Length = path1.length();
			int path2Length = path2.length();
			
			if(path1Length < path2Length) {
				return 1;
			} else if(path1Length > path2Length) {
				return -1;
			} else {
				return path1.compareTo(path2);
			}
		}
	}
}
