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

package org.apache.tajo.master.session;

import com.google.common.collect.ImmutableMap;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.common.ProtoObject;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.ipc.TajoWorkerProtocol.SessionProto;

public class Session implements SessionConstants, ProtoObject<SessionProto> {
  private final String sessionId;
  private final String userName;
  private final Map<String, String> sessionVariables;

  // transient status
  private volatile long lastAccessTime;
  private volatile String currentDatabase;

  public Session(String sessionId, String userName, String databaseName) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.lastAccessTime = System.currentTimeMillis();
    this.sessionVariables = new HashMap<String, String>();
    selectDatabase(databaseName);
  }

  public Session(SessionProto proto) {
    sessionId = proto.getSessionId();
    userName = proto.getUsername();
    currentDatabase = proto.getCurrentDatabase();
    lastAccessTime = proto.getLastAccessTime();
    KeyValueSet keyValueSet = new KeyValueSet(proto.getVariables());
    sessionVariables = keyValueSet.getAllKeyValus();
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getUserName() {
    return userName;
  }

  public void updateLastAccessTime() {
    lastAccessTime = System.currentTimeMillis();
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public void setVariable(String name, String value) {
    synchronized (sessionVariables) {
      sessionVariables.put(name, value);
    }
  }

  public String getVariable(String name) throws NoSuchSessionVariableException {
    synchronized (sessionVariables) {
      if (sessionVariables.containsKey(name)) {
        return sessionVariables.get(name);
      } else {
        throw new NoSuchSessionVariableException(name);
      }
    }
  }

  public String getVariable(String name, String defaultValue) {
    synchronized (sessionVariables) {
      if (sessionVariables.containsKey(name)) {
        return sessionVariables.get(name);
      } else {
        return defaultValue;
      }
    }
  }

  public void removeVariable(String name) {
    synchronized (sessionVariables) {
      sessionVariables.remove(name);
    }
  }

  public synchronized Map<String, String> getAllVariables() {
    synchronized (sessionVariables) {
      return ImmutableMap.copyOf(sessionVariables);
    }
  }

  public void selectDatabase(String databaseName) {
    this.currentDatabase = databaseName;
  }

  public String getCurrentDatabase() {
    return this.currentDatabase;
  }

  @Override
  public SessionProto getProto() {
    SessionProto.Builder builder = SessionProto.newBuilder();
    builder.setSessionId(sessionId);
    builder.setUsername(userName);
    builder.setCurrentDatabase(currentDatabase);
    builder.setLastAccessTime(lastAccessTime);
    KeyValueSet variables = new KeyValueSet();
    variables.putAll(this.sessionVariables);
    builder.setVariables(variables.getProto());
    return builder.build();
  }

  public String toString() {
    return "user=" + userName + ",id=" + sessionId;
  }
}
