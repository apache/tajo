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

package org.apache.tajo.session;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.common.ProtoObject;

import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.ipc.TajoWorkerProtocol.SessionProto;

public class Session implements SessionConstants, ProtoObject<SessionProto>, Cloneable {
  private static final Log LOG = LogFactory.getLog(Session.class);

  private final String sessionId;
  private final String userName;
  private String currentDatabase;
  private final Map<String, String> sessionVariables;
  private final Map<QueryId, NonForwardQueryResultScanner> nonForwardQueryMap = new HashMap<QueryId, NonForwardQueryResultScanner>();
  private LoadingCache<String, Expr> cache;

  // transient status
  private volatile long lastAccessTime;

  public Session(String sessionId, String userName, String databaseName) {
    this.sessionId = sessionId;
    this.userName = userName;
    this.currentDatabase = databaseName;
    this.lastAccessTime = System.currentTimeMillis();

    this.sessionVariables = new HashMap<String, String>();
    sessionVariables.put(SessionVars.SESSION_ID.keyname(), sessionId);
    sessionVariables.put(SessionVars.USERNAME.keyname(), userName);
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
      sessionVariables.put(SessionVars.handleDeprecatedName(name), value);
    }
  }

  public String getVariable(String name) throws NoSuchSessionVariableException {
    synchronized (sessionVariables) {
      if (sessionVariables.containsKey(name)) {
        return sessionVariables.get(SessionVars.handleDeprecatedName(name));
      } else {
        throw new NoSuchSessionVariableException(name);
      }
    }
  }

  public void removeVariable(String name) {
    synchronized (sessionVariables) {
      sessionVariables.remove(SessionVars.handleDeprecatedName(name));
    }
  }

  public synchronized Map<String, String> getAllVariables() {
    synchronized (sessionVariables) {
      sessionVariables.put(SessionVars.SESSION_ID.keyname(), sessionId);
      sessionVariables.put(SessionVars.USERNAME.keyname(), userName);
      sessionVariables.put(SessionVars.SESSION_LAST_ACCESS_TIME.keyname(), String.valueOf(lastAccessTime));
      sessionVariables.put(SessionVars.CURRENT_DATABASE.keyname(), currentDatabase);
      return ImmutableMap.copyOf(sessionVariables);
    }
  }

  public synchronized void selectDatabase(String databaseName) {
    this.currentDatabase = databaseName;
  }

  public synchronized String getCurrentDatabase() {
    return currentDatabase;
  }

  public synchronized void setQueryCache(LoadingCache<String, Expr> cache) {
    this.cache = cache;
  }

  public synchronized LoadingCache<String, Expr> getQueryCache() {
    return cache;
  }

  @Override
  public SessionProto getProto() {
    SessionProto.Builder builder = SessionProto.newBuilder();
    builder.setSessionId(getSessionId());
    builder.setUsername(getUserName());
    builder.setCurrentDatabase(getCurrentDatabase());
    builder.setLastAccessTime(lastAccessTime);
    KeyValueSet variables = new KeyValueSet();

    synchronized (sessionVariables) {
      variables.putAll(this.sessionVariables);
      builder.setVariables(variables.getProto());
      return builder.build();
    }
  }

  public String toString() {
    return "user=" + getUserName() + ",id=" + getSessionId() +",last_atime=" + getLastAccessTime();
  }

  public Session clone() throws CloneNotSupportedException {
    Session newSession = (Session) super.clone();
    newSession.sessionVariables.putAll(getAllVariables());
    return newSession;
  }

  public NonForwardQueryResultScanner getNonForwardQueryResultScanner(QueryId queryId) {
    synchronized (nonForwardQueryMap) {
      return nonForwardQueryMap.get(queryId);
    }
  }

  public void addNonForwardQueryResultScanner(NonForwardQueryResultScanner resultScanner) {
    synchronized (nonForwardQueryMap) {
      nonForwardQueryMap.put(resultScanner.getQueryId(), resultScanner);
    }
  }

  public void closeNonForwardQueryResultScanner(QueryId queryId) {
    NonForwardQueryResultScanner resultScanner;
    synchronized (nonForwardQueryMap) {
      resultScanner = nonForwardQueryMap.remove(queryId);
    }

    if (resultScanner != null) {
      try {
        resultScanner.close();
      } catch (Exception e) {
        LOG.error("NonForwardQueryResultScanne close error: " + e.getMessage(), e);
      }
    }
  }

  public void close() {
    try {
      synchronized (nonForwardQueryMap) {
        for (NonForwardQueryResultScanner eachQueryScanner: nonForwardQueryMap.values()) {
          try {
            eachQueryScanner.close();
          } catch (Exception e) {
            LOG.error("Error while closing NonForwardQueryResultScanner: " +
                eachQueryScanner.getSessionId() + ", " + e.getMessage(), e);
          }
        }

        nonForwardQueryMap.clear();
      }
    } catch (Throwable t) {
      LOG.error(t.getMessage(), t);
      throw new RuntimeException(t.getMessage(), t);
    }
  }
}
