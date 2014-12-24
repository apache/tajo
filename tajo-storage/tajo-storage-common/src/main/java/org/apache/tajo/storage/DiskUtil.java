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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.Util;

import java.io.*;
import java.net.URI;
import java.util.*;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

public class DiskUtil {

  static String UNIX_DISK_DEVICE_PATH = "/proc/partitions";

  public enum OSType {
		OS_TYPE_UNIX, OS_TYPE_WINXP, OS_TYPE_SOLARIS, OS_TYPE_MAC
	}

	static private OSType getOSType() {
		String osName = System.getProperty("os.name");
		if (osName.contains("Windows")
				&& (osName.contains("XP") || osName.contains("2003")
						|| osName.contains("Vista")
						|| osName.contains("Windows_7")
						|| osName.contains("Windows 7") || osName
							.contains("Windows7"))) {
			return OSType.OS_TYPE_WINXP;
		} else if (osName.contains("SunOS") || osName.contains("Solaris")) {
			return OSType.OS_TYPE_SOLARIS;
		} else if (osName.contains("Mac")) {
			return OSType.OS_TYPE_MAC;
		} else {
			return OSType.OS_TYPE_UNIX;
		}
	}
	
	public static List<DiskDeviceInfo> getDiskDeviceInfos() throws IOException {
		List<DiskDeviceInfo> deviceInfos;
		
		if(getOSType() == OSType.OS_TYPE_UNIX) {
			deviceInfos = getUnixDiskDeviceInfos();
			setDeviceMountInfo(deviceInfos);
		} else {
			deviceInfos = getDefaultDiskDeviceInfos();
		}
		
		return deviceInfos;
	}

	private static List<DiskDeviceInfo> getUnixDiskDeviceInfos() {
		List<DiskDeviceInfo> infos = new ArrayList<DiskDeviceInfo>();
		
		File file = new File(UNIX_DISK_DEVICE_PATH);
		if(!file.exists()) {
			System.out.println("No partition file:" + file.getAbsolutePath());
			return getDefaultDiskDeviceInfos();
		}
		
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(UNIX_DISK_DEVICE_PATH)));
			String line = null;
			
			int count = 0;
			Set<String> deviceNames = new TreeSet<String>();
			while((line = reader.readLine()) != null) {
				if(count > 0 && !line.trim().isEmpty()) {
					String[] tokens = line.trim().split(" +");
					if(tokens.length == 4) {
						String deviceName = getDiskDeviceName(tokens[3]);
						deviceNames.add(deviceName);
					}
				}
				count++;
			}
			
			int id = 0;
			for(String eachDeviceName: deviceNames) {
				DiskDeviceInfo diskDeviceInfo = new DiskDeviceInfo(id++);
				diskDeviceInfo.setName(eachDeviceName);
				
				//TODO set addtional info
				// /sys/block/sda/queue
				infos.add(diskDeviceInfo);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
				}
			}
		}
		
		return infos;
	}
	
	private static String getDiskDeviceName(String partitionName) {
		byte[] bytes = partitionName.getBytes();
		
		byte[] result = new byte[bytes.length];
		int length = 0;
		for(int i = 0; i < bytes.length; i++, length++) {
			if(bytes[i] >= '0' && bytes[i] <= '9') {
				break;
			} else {
				result[i] = bytes[i];
			}
		}
		
		return new String(result, 0, length);
	}
	
	public static List<DiskDeviceInfo> getDefaultDiskDeviceInfos() {
		DiskDeviceInfo diskDeviceInfo = new DiskDeviceInfo(0);
		diskDeviceInfo.setName("default");
		
		List<DiskDeviceInfo> infos = new ArrayList<DiskDeviceInfo>();
		
		infos.add(diskDeviceInfo);
		
		return infos;
	}
	
	
	private static void setDeviceMountInfo(List<DiskDeviceInfo> deviceInfos) throws IOException {
		Map<String, DiskDeviceInfo> deviceMap = new HashMap<String, DiskDeviceInfo>();
		for(DiskDeviceInfo eachDevice: deviceInfos) {
			deviceMap.put(eachDevice.getName(), eachDevice);
		}
		
		BufferedReader mountOutput = null;
		try {
			Process mountProcess = Runtime.getRuntime().exec("mount");
			mountOutput = new BufferedReader(new InputStreamReader(
					mountProcess.getInputStream()));
			while (true) {
				String line = mountOutput.readLine();
				if (line == null) {
					break;
				}

				int indexStart = line.indexOf(" on /");
				int indexEnd = line.indexOf(" ", indexStart + 4);

				String deviceName = line.substring(0, indexStart).trim();
				String[] deviceNameTokens = deviceName.split("/");
				if(deviceNameTokens.length == 3) {
					if("dev".equals(deviceNameTokens[1])) {
						String realDeviceName = getDiskDeviceName(deviceNameTokens[2]);
						String mountPath = new File(line.substring(indexStart + 4, indexEnd)).getAbsolutePath();
						
						DiskDeviceInfo diskDeviceInfo = deviceMap.get(realDeviceName);
						if(diskDeviceInfo != null) {
							diskDeviceInfo.addMountPath(new DiskMountInfo(diskDeviceInfo.getId(), mountPath));
						}
					}
				}
			}
		} catch (IOException e) {
			throw e;
		} finally {
			if (mountOutput != null) {
				mountOutput.close();
			}
		}
	}

  public static int getDataNodeStorageSize(){
    return getStorageDirs().size();
  }

  public static List<URI> getStorageDirs(){
    Configuration conf = new HdfsConfiguration();
    Collection<String> dirNames = conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    return Util.stringCollectionAsURIs(dirNames);
  }

	public static void main(String[] args) throws Exception {
		System.out.println("/dev/sde1".split("/").length);
		for(String eachToken: "/dev/sde1".split("/")) {
			System.out.println(eachToken);
		}
 	}
}
