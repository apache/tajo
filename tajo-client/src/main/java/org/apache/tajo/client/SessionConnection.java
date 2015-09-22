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
import io.netty.channel.EventLoopGroup;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.client.v2.exception.ClientConnectionException;
import org.apache.tajo.exception.ExceptionUtil;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.SessionUpdateResponse;
import org.apache.tajo.ipc.ClientProtos.UpdateSessionVariableRequest;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NettyUtils;
import org.apache.tajo.rpc.RpcClientManager;
import org.apache.tajo.rpc.RpcConstants;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetResponse;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.ReturnState;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.StringResponse;
import org.apache.tajo.service.ServiceTracker;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.ProtoUtil;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.error.Errors.ResultCode.NO_SUCH_SESSION_VARIABLE;
import static org.apache.tajo.exception.ReturnStateUtil.*;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionRequest;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionResponse;

public class SessionConnection implements Closeable {

  private final static Log LOG = LogFactory.getLog(SessionConnection.class);

  final RpcClientManager manager;

  private String baseDatabase;

  private final UserRoleInfo userInfo;

  volatile TajoIdProtos.SessionIdProto sessionId;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  /** session variable cache */
  private final Map<String, String> sessionVarsCache = new HashMap<String, String>();

  private final ServiceTracker serviceTracker;

  private final EventLoopGroup eventLoopGroup;

  private NettyClientBase client;

  private final KeyValueSet properties;

  /**
   * Connect to TajoMaster
   *
   * @param tracker TajoMaster address
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @param properties configurations
   * @throws SQLException
   */
  public SessionConnection(@NotNull ServiceTracker tracker, @Nullable String baseDatabase,
                           @NotNull KeyValueSet properties) {
    this.serviceTracker = tracker;
    this.baseDatabase = baseDatabase;
    this.properties = properties;

    this.manager = RpcClientManager.getInstance();
    this.manager.setRetries(properties.getInt(RpcConstants.RPC_CLIENT_RETRY_MAX, RpcConstants.DEFAULT_RPC_RETRIES));
    this.userInfo = UserRoleInfo.getCurrentUser();

    this.eventLoopGroup = NettyUtils.createEventLoopGroup(getClass().getSimpleName(), 4);
    try {
      this.client = getTajoMasterConnection();
    } catch (TajoRuntimeException e) {
      NettyUtils.shutdown(eventLoopGroup);
      throw e;
    }
  }

  public Map<String, String> getClientSideSessionVars() {
    return Collections.unmodifiableMap(sessionVarsCache);
  }

  public synchronized NettyClientBase getTajoMasterConnection() {

    if (client != null && client.isConnected()) {
      return client;
    } else {

      try {
        RpcClientManager.cleanup(client);

        // Client do not closed on idle state for support high available
        this.client = manager.newBlockingClient(getTajoMasterAddr(), TajoMasterClientProtocol.class,
            manager.getRetries(), eventLoopGroup);
      } catch (Throwable t) {
        throw new TajoRuntimeException(new ClientConnectionException(t));
      }

      return client;
    }
  }

  protected BlockingInterface getTMStub() {
    NettyClientBase tmClient;
    tmClient = getTajoMasterConnection();
    BlockingInterface stub = tmClient.getStub();
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

  public String getCurrentDatabase() {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface tajoMasterService = client.getStub();

    StringResponse response;
    try {
      response = tajoMasterService.getCurrentDatabase(null, sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    return response.getValue();
  }

  public Map<String, String> updateSessionVariables(final Map<String, String> variables) {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface tajoMasterService = client.getStub();
    KeyValueSet keyValueSet = new KeyValueSet();
    keyValueSet.putAll(variables);
    UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
        .setSessionId(sessionId)
        .setSessionVars(keyValueSet.getProto()).build();

    SessionUpdateResponse response;

    try {
      response = tajoMasterService.updateSessionVariables(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
    properties.putAll(sessionVarsCache);
    return Collections.unmodifiableMap(sessionVarsCache);
  }

  public Map<String, String> unsetSessionVariables(final List<String> variables) {

    final BlockingInterface stub = getTMStub();
    final UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
        .setSessionId(sessionId)
        .addAllUnsetVariables(variables)
        .build();

    SessionUpdateResponse response;
    try {
      response = stub.updateSessionVariables(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
    return Collections.unmodifiableMap(sessionVarsCache);
  }

  void updateSessionVarsCache(Map<String, String> variables) {
    synchronized (sessionVarsCache) {
      this.sessionVarsCache.clear();
      this.sessionVarsCache.putAll(variables);
    }
  }

  public String getSessionVariable(final String varname) throws NoSuchSessionVariableException {
    synchronized (sessionVarsCache) {
      // If a desired variable is client side one and exists in the cache, immediately return the variable.
      if (sessionVarsCache.containsKey(varname)) {
        return sessionVarsCache.get(varname);
      }
    }

    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface stub = client.getStub();
    StringResponse response;
    try {
      response = stub.getSessionVariable(null, getSessionedString(varname));

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isThisError(response.getState(), NO_SUCH_SESSION_VARIABLE)) {
      throw new NoSuchSessionVariableException(response.getState());
    }

    ensureOk(response.getState());
    return response.getValue();
  }

  public boolean existSessionVariable(final String varname) {

    ReturnState state;
    try {
      final BlockingInterface stub = getTMStub();
      state = stub.existSessionVariable(null, getSessionedString(varname));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    if (isThisError(state, NO_SUCH_SESSION_VARIABLE)) {
      return false;
    }

    ensureOk(state);
    return true;
  }

  public Map<String, String> getAllSessionVariables() {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface stub = client.getStub();
    KeyValueSetResponse response;
    try {
      response = stub.getAllSessionVariables(null, sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    ensureOk(response.getState());
    return ProtoUtil.convertToMap(response.getValue());
  }

  public void selectDatabase(final String dbName) throws UndefinedDatabaseException {

    try {
      final BlockingInterface stub = getTMStub();
      final ReturnState state = stub.selectDatabase(null, getSessionedString(dbName));

      ExceptionUtil.throwsIfThisError(state, UndefinedDatabaseException.class);
      ensureOk(state);
      this.baseDatabase = dbName;

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
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
      BlockingInterface tajoMaster = client.getStub();
      tajoMaster.removeSession(null, sessionId);
    } catch (Throwable e) {
      // ignore
    } finally {
      RpcClientManager.cleanup(client);
      NettyUtils.shutdown(eventLoopGroup);
    }
  }

  protected InetSocketAddress getTajoMasterAddr() {
    return serviceTracker.getClientServiceAddress();
  }

  protected void checkSessionAndGet(NettyClientBase client) {

    if (sessionId == null) {

      BlockingInterface tajoMasterService = client.getStub();
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
        throw new TajoRuntimeException(response.getState());
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
    BlockingInterface tajoMasterService = client.getStub();
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
      UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
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
