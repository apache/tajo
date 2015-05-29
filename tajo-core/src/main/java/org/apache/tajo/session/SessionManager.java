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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager extends CompositeService implements EventHandler<SessionEvent> {
  private static final Log LOG = LogFactory.getLog(SessionManager.class);

  public final ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<String, Session>();
  private final Dispatcher dispatcher;
  private SessionLivelinessMonitor sessionLivelinessMonitor;


  public SessionManager(Dispatcher dispatcher) {
    super(SessionManager.class.getSimpleName());
    this.dispatcher = dispatcher;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    sessionLivelinessMonitor = new SessionLivelinessMonitor(dispatcher);
    addIfService(sessionLivelinessMonitor);
    super.serviceInit(conf);
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  private void assertSessionExistence(String sessionId) throws InvalidSessionException {
    if (!sessions.containsKey(sessionId)) {
      throw new InvalidSessionException(sessionId);
    }
  }

  public String createSession(String username, String baseDatabaseName) throws InvalidSessionException {
    String sessionId;
    Session oldSession;

    sessionId = UUID.randomUUID().toString();
    Session newSession = new Session(sessionId, username, baseDatabaseName);
    oldSession = sessions.putIfAbsent(sessionId, newSession);
    if (oldSession != null) {
      throw new InvalidSessionException("Session id is duplicated: " + oldSession.getSessionId());
    }
    LOG.info("Session " + sessionId + " is created." );
    return sessionId;
  }

  public Session removeSession(String sessionId) {
    if (sessions.containsKey(sessionId)) {
      LOG.info("Session " + sessionId + " is removed.");
      Session session = sessions.remove(sessionId);
      session.close();
      return session;
    } else {
      LOG.error("No such session id: " + sessionId);
      return null;
    }
  }

  public Session getSession(String sessionId) throws InvalidSessionException {
    assertSessionExistence(sessionId);
    touch(sessionId);
    return sessions.get(sessionId);
  }

  public void setVariable(String sessionId, String name, String value) throws InvalidSessionException {
    assertSessionExistence(sessionId);
    touch(sessionId);
    sessions.get(sessionId).setVariable(name, value);
  }

  public String getVariable(String sessionId, String name)
      throws InvalidSessionException, NoSuchSessionVariableException {
    assertSessionExistence(sessionId);
    touch(sessionId);
    return sessions.get(sessionId).getVariable(name);
  }

  public void removeVariable(String sessionId, String name) throws InvalidSessionException {
    assertSessionExistence(sessionId);
    touch(sessionId);
    sessions.get(sessionId).removeVariable(name);
  }

  public Map<String, String> getAllVariables(String sessionId) throws InvalidSessionException {
    assertSessionExistence(sessionId);
    touch(sessionId);
    return sessions.get(sessionId).getAllVariables();
  }

  public void touch(String sessionId) throws InvalidSessionException {
    assertSessionExistence(sessionId);
    sessions.get(sessionId).updateLastAccessTime();
    sessionLivelinessMonitor.receivedPing(sessionId);
  }

  @Override
  public void handle(SessionEvent event) {
    LOG.info("Processing " + event.getSessionId() + " of type " + event.getType());

    try {
      assertSessionExistence(event.getSessionId());
      touch(event.getSessionId());
    } catch (InvalidSessionException e) {
      LOG.error(e, e);
    }

    if (event.getType() == SessionEventType.EXPIRE) {
      Session session = removeSession(event.getSessionId());
      if (session != null) {
        LOG.info("[Expired] Session username=" + session.getUserName() + ",sessionid=" + event.getSessionId());
      }
    }
  }
}
