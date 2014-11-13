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

package org.apache.tajo.master.cluster;

public class ServerName implements Comparable<ServerName> {
  /**
   * This character is used as separator between server hostname and port.
   */
  public static final String SERVERNAME_SEPARATOR = ":";

  private final String serverName;
  private final String hostname;
  private final int port;


  public ServerName(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
    this.serverName = getServerName(hostname, port);
  }

  public ServerName(final String serverName) {
    this(parseHostname(serverName), parsePort(serverName));
  }
  
  public static ServerName create(final String serverName) {
	  return new ServerName(serverName);
  }

  public static ServerName createWithDefaultPort(final String serverName,
                                                 final int defaultPort) {
    if (serverName == null || serverName.length() <= 0) {
      throw new IllegalArgumentException("Passed hostname is null or empty ("
          + serverName + ")");
    }
    int index = serverName.indexOf(SERVERNAME_SEPARATOR);
    if (index == -1) {
      return new ServerName(parseHostname(serverName), defaultPort);
    } else {
      return new ServerName(parseHostname(serverName), parsePort(serverName));
    }
  }

  public static String parseHostname(final String serverName) {
    if (serverName == null || serverName.length() <= 0) {
      throw new IllegalArgumentException("Passed hostname is null or empty ("
          + serverName + ")");
    }
    int index = serverName.indexOf(SERVERNAME_SEPARATOR);
    if (index == -1) { // if a port is missing, the index will be set to -1.
      throw new IllegalArgumentException("Passed port is missing ("
          + serverName + ")");
    }
    return serverName.substring(0, index);
  }

  public static int parsePort(final String serverName) {
    String [] split = serverName.split(SERVERNAME_SEPARATOR);
    return Integer.parseInt(split[1]);
  }

  @Override
  public String toString() {
    return getServerName();
  }

  public String getServerName() {
    return serverName;
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  public static String getServerName(String hostName, int port) {
    final StringBuilder name = new StringBuilder(hostName.length() + 8);
    name.append(hostName);
    name.append(SERVERNAME_SEPARATOR);
    name.append(port);
    return name.toString();
  }

  @Override
  public int compareTo(ServerName other) {
    int compare = this.getHostname().toLowerCase().
      compareTo(other.getHostname().toLowerCase());
    if (compare != 0) return compare;
    return this.getPort() - other.getPort();        
  }

  @Override
  public int hashCode() {
    return getServerName().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;
    if (!(o instanceof ServerName)) return false;
    return this.compareTo((ServerName)o) == 0;
  }
}
