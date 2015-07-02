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

package org.apache.tajo.client;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.SessionUpdateResponse;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcChannelFactory;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.RpcConstants;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetResponse;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringResponse;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.ProtoUtil;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.client.ClientErrorUtil.isError;
import static org.apache.tajo.client.ClientErrorUtil.isSuccess;
import static org.apache.tajo.client.SQLExceptionUtil.toSQLException;
import static org.apache.tajo.client.SQLExceptionUtil.throwIfError;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionRequest;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionResponse;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;

public class SessionConnection implements Closeable {

  private final static Log LOG = LogFactory.getLog(SessionConnection.class);

  private final static AtomicInteger connections = new AtomicInteger();

  final RpcClientManager manager;

  private String baseDatabase;

  private final UserRoleInfo userInfo;

  volatile TajoIdProtos.SessionIdProto sessionId;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** session variable cache */
  private final Map<String, String> sessionVarsCache = new HashMap<String, String>();

  private final ServiceTracker serviceTracker;

  private NettyClientBase client;

  private final KeyValueSet properties;

  /**
   * Connect to TajoMaster
   *
   * @param tracker TajoMaster address
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @param properties configurations
   * @throws java.io.IOException
   */
  public SessionConnection(@NotNull ServiceTracker tracker, @Nullable String baseDatabase,
                           @NotNull KeyValueSet properties) throws SQLException {
    this.serviceTracker = tracker;
    this.baseDatabase = baseDatabase;
    this.properties = properties;

    this.manager = RpcClientManager.getInstance();
    this.manager.setRetries(properties.getInt(RpcConstants.RPC_CLIENT_RETRY_MAX, RpcConstants.DEFAULT_RPC_RETRIES));
    this.userInfo = UserRoleInfo.getCurrentUser();

    this.client = getTajoMasterConnection();
  }

  public Map<String, String> getClientSideSessionVars() {
    return Collections.unmodifiableMap(sessionVarsCache);
  }

  public synchronized NettyClientBase getTajoMasterConnection() throws SQLException {

    if (client != null && client.isConnected()) {
      return client;
    } else {

      try {
        RpcClientManager.cleanup(client);

        // Client do not closed on idle state for support high available
        this.client = manager.newClient(
            getTajoMasterAddr(),
            TajoMasterClientProtocol.class,
            false,
            manager.getRetries(),
            0,
            TimeUnit.SECONDS,
            false);
        connections.incrementAndGet();

      } catch (Throwable t) {
        throw SQLExceptionUtil.makeUnableToEstablishConnection(t);
      }

      return client;
    }
  }

  protected TajoMasterClientProtocolService.BlockingInterface getTMStub() throws SQLException {
    NettyClientBase tmClient;
    tmClient = getTajoMasterConnection();
    TajoMasterClientProtocolService.BlockingInterface stub = tmClient.getStub();
    checkSessionAndGet(tmClient);
    return stub;
  }

  public KeyValueSet getProperties() {
    return properties;
  }

  @SuppressWarnings("unused")
  public void setSessionId(TajoIdProtos.SessionIdProto sessionId) {
    this.sessionId = sessionId;
  }

  public String getSessionId() {
    return sessionId.getId();
  }

  public String getBaseDatabase() {
    return baseDatabase;
  }

  public boolean isConnected() {
    if (!closed.get()) {
      try {
        return getTajoMasterConnection().isConnected();
      } catch (Throwable e) {
        return false;
      }
    }
    return false;
  }

  public UserRoleInfo getUserInfo() {
    return userInfo;
  }

  public String getCurrentDatabase() throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();

    StringResponse response;
    try {
      response = tajoMasterService.getCurrentDatabase(null, sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    return response.getValue();
  }

  public Map<String, String> updateSessionVariables(final Map<String, String> variables) throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    KeyValueSet keyValueSet = new KeyValueSet();
    keyValueSet.putAll(variables);
    ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
        .setSessionId(sessionId)
        .setSessionVars(keyValueSet.getProto()).build();

    SessionUpdateResponse response;

    try {
      response = tajoMasterService.updateSessionVariables(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isSuccess(response.getState())) {
      updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return Collections.unmodifiableMap(sessionVarsCache);
    } else {
      throw toSQLException(response.getState());
    }
  }

  public Map<String, String> unsetSessionVariables(final List<String> variables) throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
        .setSessionId(sessionId)
        .addAllUnsetVariables(variables).build();

    SessionUpdateResponse response;

    try {
      response = tajoMasterService.updateSessionVariables(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isSuccess(response.getState())) {
      updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return Collections.unmodifiableMap(sessionVarsCache);
    } else {
      throw toSQLException(response.getState());
    }
  }

  void updateSessionVarsCache(Map<String, String> variables) {
    synchronized (sessionVarsCache) {
      this.sessionVarsCache.clear();
      this.sessionVarsCache.putAll(variables);
    }
  }

  public String getSessionVariable(final String varname) throws SQLException {
    synchronized (sessionVarsCache) {
      // If a desired variable is client side one and exists in the cache, immediately return the variable.
      if (sessionVarsCache.containsKey(varname)) {
        return sessionVarsCache.get(varname);
      }
    }

    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface stub = client.getStub();

    try {
      return stub.getSessionVariable(null, getSessionedString(varname)).getValue();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public Boolean existSessionVariable(final String varname) throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface stub = client.getStub();
    try {
      return isSuccess(stub.existSessionVariable(null, getSessionedString(varname)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, String> getAllSessionVariables() throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface stub = client.getStub();
    KeyValueSetResponse response;
    try {
      response = stub.getAllSessionVariables(null, sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    return ProtoUtil.convertToMap(response.getValue());
  }

  public Boolean selectDatabase(final String databaseName) throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    TajoMasterClientProtocolService.BlockingInterface stub = client.getStub();
    boolean selected = false;
    try {
      selected = isSuccess(stub.selectDatabase(null, getSessionedString(databaseName)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (selected) {
      this.baseDatabase = databaseName;
    }
    return selected;
  }

  @Override
  public void close() {
    if (closed.getAndSet(true)) {
      return;
    }

    // remove session
    NettyClientBase client = null;
    try {
      client = getTajoMasterConnection();
      TajoMasterClientProtocolService.BlockingInterface tajoMaster = client.getStub();
      tajoMaster.removeSession(null, sessionId);
    } catch (Throwable e) {
      // ignore
    } finally {
      RpcClientManager.cleanup(client);
      if(connections.decrementAndGet() == 0) {
        if (!System.getProperty(CommonTestingUtil.TAJO_TEST_KEY, "FALSE").equals(CommonTestingUtil.TAJO_TEST_TRUE)) {
          RpcChannelFactory.shutdownGracefully();
          if (LOG.isDebugEnabled()) {
            LOG.debug("RPC connection is closed");
          }
        }
      }
    }
  }

  protected InetSocketAddress getTajoMasterAddr() {
    return serviceTracker.getClientServiceAddress();
  }

  protected void checkSessionAndGet(NettyClientBase client) throws SQLException {

    if (sessionId == null) {

      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setUsername(userInfo.getUserName()).build();

      if (baseDatabase != null) {
        builder.setBaseDatabaseName(baseDatabase);
      }


      CreateSessionResponse response = null;

      try {
        response = tajoMasterService.createSession(null, builder.build());
      } catch (ServiceException se) {
        throw new RuntimeException(se);
      }

      if (isSuccess(response.getState())) {

        sessionId = response.getSessionId();
        updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
        }
      } else {
        throw SQLExceptionUtil.toSQLException(response.getState());
      }
    }
  }

  public boolean reconnect() throws Exception {
    CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
    builder.setUsername(userInfo.getUserName()).build();
    if (baseDatabase != null) {
      builder.setBaseDatabaseName(baseDatabase);
    }

    NettyClientBase client = getTajoMasterConnection();

    // create new session
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());
    if (isError(response.getState())) {
      return false;
    }

    // Invalidate some session variables in client cache
    sessionId = response.getSessionId();
    Map<String, String> sessionVars = ProtoUtil.convertToMap(response.getSessionVars());
    synchronized (sessionVarsCache) {
      for (SessionVars var : UPDATE_ON_RECONNECT) {
        String value = sessionVars.get(var.keyname());
        if (value != null) {
          sessionVarsCache.put(var.keyname(), value);
        }
      }
    }

    // Update the session variables in server side
    try {
      KeyValueSet keyValueSet = new KeyValueSet();
      keyValueSet.putAll(sessionVarsCache);
      ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
          .setSessionId(sessionId)
          .setSessionVars(keyValueSet.getProto()).build();

      if (isError(tajoMasterService.updateSessionVariables(null, request).getState())) {
        tajoMasterService.removeSession(null, sessionId);
        return false;
      }
      LOG.info(String.format("Reconnected to session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
      return true;
    } catch (ServiceException e) {
      tajoMasterService.removeSession(null, sessionId);
      return false;
    }
  }

  /**
   * Session variables which should be updated upon reconnecting
   */
  private static final SessionVars[] UPDATE_ON_RECONNECT = new SessionVars[] {
    SessionVars.SESSION_ID, SessionVars.SESSION_LAST_ACCESS_TIME, SessionVars.CLIENT_HOST
  };

  ClientProtos.SessionedStringProto getSessionedString(String str) {
    ClientProtos.SessionedStringProto.Builder builder = ClientProtos.SessionedStringProto.newBuilder();
    builder.setSessionId(sessionId);
    if (str != null) {
      builder.setValue(str);
    }
    return builder.build();
  }

}
