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
import org.apache.tajo.exception.SQLExceptionUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.SessionUpdateResponse;
import org.apache.tajo.ipc.ClientProtos.UpdateSessionVariableRequest;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService.BlockingInterface;
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

import static org.apache.tajo.exception.ReturnStateUtil.isError;
import static org.apache.tajo.exception.ReturnStateUtil.isSuccess;
import static org.apache.tajo.exception.SQLExceptionUtil.toSQLException;
import static org.apache.tajo.exception.SQLExceptionUtil.throwIfError;
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
   * @throws SQLException
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

  protected BlockingInterface getTMStub() throws SQLException {
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

  public String getCurrentDatabase() throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface tajoMasterService = client.getStub();

    StringResponse response;
    try {
      response = tajoMasterService.getCurrentDatabase(null, sessionId);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

    throwIfError(response.getState());
    return response.getValue();
  }

<<<<<<< HEAD
  public Map<String, String> updateSessionVariables(final Map<String, String> variables) throws ServiceException {
//<<<<<<< HEAD
//    return new ServerCallable<Map<String, String>>(manager, getTajoMasterAddr(),
//        TajoMasterClientProtocol.class, false) {
//
//      public Map<String, String> call(NettyClientBase client) throws ServiceException {
//        checkSessionAndGet(client);
//
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//        KeyValueSet keyValueSet = new KeyValueSet();
//        keyValueSet.putAll(variables);
//        ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
//            .setSessionId(sessionId)
//            .setSessionVars(keyValueSet.getProto()).build();
//
//        SessionUpdateResponse response = tajoMasterService.updateSessionVariables(null, request);
//
//        if (response.getResult().getResultCode() == ResultCode.OK) {
//          updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
//          return Collections.unmodifiableMap(sessionVarsCache);
//        } else {
//          throw new ServiceException(response.getResult().getErrorMessage());
//        }
//      }
//    }.withRetries();
//  }
//=======
=======
  public Map<String, String> updateSessionVariables(final Map<String, String> variables) throws SQLException {
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746

    BlockingInterface tajoMasterService = client.getStub();
    KeyValueSet keyValueSet = new KeyValueSet();
    keyValueSet.putAll(variables);
    UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
        .setSessionId(sessionId)
        .setSessionVars(keyValueSet.getProto()).build();

    SessionUpdateResponse response;

<<<<<<< HEAD
    if (response.getResult().getResultCode() == ResultCode.OK) {
      updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return Collections.unmodifiableMap(sessionVarsCache);
    } else {
      throw new ServiceException(response.getResult().getErrorMessage());
=======
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
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    }
  }

  public Map<String, String> unsetSessionVariables(final List<String> variables) throws SQLException {

<<<<<<< HEAD
//<<<<<<< HEAD
//        if (response.getResult().getResultCode() == ResultCode.OK) {
//          updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
//          return Collections.unmodifiableMap(sessionVarsCache);
//        } else {
//          throw new ServiceException(response.getResult().getErrorMessage());
//        }
//      }
//    }.withRetries();
//=======
    TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
    ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
=======
    final BlockingInterface stub = getTMStub();
    final UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
        .setSessionId(sessionId)
        .addAllUnsetVariables(variables)
        .build();

    SessionUpdateResponse response;
    try {
      response = stub.updateSessionVariables(null, request);
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }

<<<<<<< HEAD
    if (response.getResult().getResultCode() == ResultCode.OK) {
      updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return Collections.unmodifiableMap(sessionVarsCache);
    } else {
      throw new ServiceException(response.getResult().getErrorMessage());
=======
    if (isSuccess(response.getState())) {
      updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
      return Collections.unmodifiableMap(sessionVarsCache);
    } else {
      throw toSQLException(response.getState());
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
    }
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
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

    BlockingInterface stub = client.getStub();

    try {
      return stub.getSessionVariable(null, getSessionedString(varname)).getValue();

    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public Boolean existSessionVariable(final String varname) throws SQLException {

    BlockingInterface stub = getTMStub();
    try {
      return isSuccess(stub.existSessionVariable(null, getSessionedString(varname)));
    } catch (ServiceException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, String> getAllSessionVariables() throws SQLException {
    NettyClientBase client = getTajoMasterConnection();
    checkSessionAndGet(client);

    BlockingInterface stub = client.getStub();
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

    BlockingInterface stub = getTMStub();
    boolean selected;
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
      BlockingInterface tajoMaster = client.getStub();
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

      BlockingInterface tajoMasterService = client.getStub();
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setUsername(userInfo.getUserName()).build();

      if (baseDatabase != null) {
        builder.setBaseDatabaseName(baseDatabase);
      }


<<<<<<< HEAD
      if (response.getResult().getResultCode() == ResultCode.OK) {
=======
      CreateSessionResponse response = null;

      try {
        response = tajoMasterService.createSession(null, builder.build());
      } catch (ServiceException se) {
        throw new RuntimeException(se);
      }

      if (isSuccess(response.getState())) {
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867

        sessionId = response.getSessionId();
        updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
        }
      } else {
<<<<<<< HEAD
        throw new InvalidClientSessionException(response.getResult().getErrorMessage());
=======
        throw SQLExceptionUtil.toSQLException(response.getState());
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
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

//<<<<<<< HEAD
//        // create new session
//        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
//        CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());
//        if (response.getResult().getResultCode() != ResultCode.OK) {
//          return false;
//        }
//=======
    // create new session
    BlockingInterface tajoMasterService = client.getStub();
    CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());
<<<<<<< HEAD
    if (response.getResult().getResultCode() != ResultCode.OK) {
=======
    if (isError(response.getState())) {
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
      return false;
    }
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746

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

//<<<<<<< HEAD
//        // Update the session variables in server side
//        try {
//          KeyValueSet keyValueSet = new KeyValueSet();
//          keyValueSet.putAll(sessionVarsCache);
//          ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
//              .setSessionId(sessionId)
//              .setSessionVars(keyValueSet.getProto()).build();
//
//          if (tajoMasterService.updateSessionVariables(null, request).getResult().getResultCode() != ResultCode.OK) {
//            tajoMasterService.removeSession(null, sessionId);
//            return false;
//          }
//          LOG.info(String.format("Reconnected to session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
//          return true;
//        } catch (ServiceException e) {
//          tajoMasterService.removeSession(null, sessionId);
//          return false;
//        }
//=======
    // Update the session variables in server side
    try {
      KeyValueSet keyValueSet = new KeyValueSet();
      keyValueSet.putAll(sessionVarsCache);
      UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
          .setSessionId(sessionId)
          .setSessionVars(keyValueSet.getProto()).build();

<<<<<<< HEAD
      if (tajoMasterService.updateSessionVariables(null, request).getResult().getResultCode() != ResultCode.OK) {
=======
      if (isError(tajoMasterService.updateSessionVariables(null, request).getState())) {
>>>>>>> c50a5dadff90fa90709abbce59856e834baa4867
        tajoMasterService.removeSession(null, sessionId);
        return false;
//>>>>>>> 9b3824b5f0c64af42bfcf0a6bb8d3555c22c5746
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
