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
import org.apache.tajo.QueryId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.auth.UserRoleInfo;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ha.HAServiceUtil;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.ResultCode;
import org.apache.tajo.ipc.ClientProtos.SessionUpdateResponse;
import org.apache.tajo.ipc.TajoMasterClientProtocol;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.ProtoUtil;
import org.jboss.netty.channel.ConnectTimeoutException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tajo.ipc.ClientProtos.CreateSessionRequest;
import static org.apache.tajo.ipc.ClientProtos.CreateSessionResponse;
import static org.apache.tajo.ipc.TajoMasterClientProtocol.TajoMasterClientProtocolService;

public class SessionConnection implements Closeable {

  private final Log LOG = LogFactory.getLog(TajoClientImpl.class);

  private final TajoConf conf;

  final Map<QueryId, InetSocketAddress> queryMasterMap = new ConcurrentHashMap<QueryId, InetSocketAddress>();

  final InetSocketAddress tajoMasterAddr;

  final RpcConnectionPool connPool;

  private final String baseDatabase;

  private final UserRoleInfo userInfo;

  volatile TajoIdProtos.SessionIdProto sessionId;

  private AtomicBoolean closed = new AtomicBoolean(false);

  /** session variable cache */
  private final Map<String, String> sessionVarsCache = new HashMap<String, String>();


  public SessionConnection(TajoConf conf) throws IOException {
    this(conf, NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)), null);
  }

  public SessionConnection(TajoConf conf, @Nullable String baseDatabase) throws IOException {
    this(conf, NetUtils.createSocketAddr(conf.getVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)), baseDatabase);
  }

  public SessionConnection(InetSocketAddress addr) throws IOException {
    this(new TajoConf(), addr, null);
  }

  public SessionConnection(String hostname, int port, String baseDatabase) throws IOException {
    this(new TajoConf(), NetUtils.createSocketAddr(hostname, port), baseDatabase);
  }

  /**
   * Connect to TajoMaster
   *
   * @param conf TajoConf
   * @param addr TajoMaster address
   * @param baseDatabase The base database name. It is case sensitive. If it is null,
   *                     the 'default' database will be used.
   * @throws java.io.IOException
   */
  public SessionConnection(TajoConf conf, InetSocketAddress addr, @Nullable String baseDatabase) throws IOException {
    this.conf = conf;
    this.conf.set("tajo.disk.scheduler.report.interval", "0");
    this.tajoMasterAddr = addr;
    int workerNum = conf.getIntVar(TajoConf.ConfVars.RPC_CLIENT_WORKER_THREAD_NUM);
    // Don't share connection pool per client
    connPool = RpcConnectionPool.newPool(conf, getClass().getSimpleName(), workerNum);
    userInfo = UserRoleInfo.getCurrentUser();
    this.baseDatabase = baseDatabase != null ? baseDatabase : null;
  }

  public Map<String, String> getClientSideSessionVars() {
    return Collections.unmodifiableMap(sessionVarsCache);
  }

  public <T> T getStub(QueryId queryId, Class protocolClass, boolean asyncMode) throws NoSuchMethodException,
      ConnectTimeoutException, ClassNotFoundException {
    InetSocketAddress addr = queryMasterMap.get(queryId);
    return connPool.getConnection(addr, protocolClass, asyncMode).getStub();
  }

  public NettyClientBase getTajoMasterConnection(boolean asyncMode) throws NoSuchMethodException,
      ConnectTimeoutException, ClassNotFoundException {
    return connPool.getConnection(getTajoMasterAddr(), TajoMasterClientProtocol.class, asyncMode);
  }

  public NettyClientBase getConnection(QueryId queryId, Class protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ConnectTimeoutException, ClassNotFoundException {
    InetSocketAddress addr = queryMasterMap.get(queryId);
    return connPool.getConnection(addr, protocolClass, asyncMode);
  }

  public NettyClientBase getConnection(InetSocketAddress addr, Class protocolClass, boolean asyncMode)
      throws NoSuchMethodException, ConnectTimeoutException, ClassNotFoundException {
    return connPool.getConnection(addr, protocolClass, asyncMode);
  }

  @SuppressWarnings("unused")
  public void setSessionId(TajoIdProtos.SessionIdProto sessionId) {
    this.sessionId = sessionId;
  }

  public TajoIdProtos.SessionIdProto getSessionId() {
    return sessionId;
  }

  public String getBaseDatabase() {
    return baseDatabase;
  }

  public boolean isConnected() {
    if(!closed.get()){
      try {
        return connPool.getConnection(tajoMasterAddr, TajoMasterClientProtocol.class, false).isConnected();
      } catch (Throwable e) {
        return false;
      }
    }
    return false;
  }

  public TajoConf getConf() {
    return conf;
  }

  public UserRoleInfo getUserInfo() {
    return userInfo;
  }

  public String getCurrentDatabase() throws ServiceException {
    return new ServerCallable<String>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getCurrentDatabase(null, sessionId).getValue();
      }
    }.withRetries();
  }

  public Map<String, String> updateSessionVariables(final Map<String, String> variables) throws ServiceException {
    return new ServerCallable<Map<String, String>>(connPool, getTajoMasterAddr(),
        TajoMasterClientProtocol.class, false, true) {

      public Map<String, String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        KeyValueSet keyValueSet = new KeyValueSet();
        keyValueSet.putAll(variables);
        ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .setSessionVars(keyValueSet.getProto()).build();

        SessionUpdateResponse response = tajoMasterService.updateSessionVariables(null, request);

        if (response.getResultCode() == ResultCode.OK) {
          updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
          return Collections.unmodifiableMap(sessionVarsCache);
        } else {
          throw new ServiceException(response.getMessage());
        }
      }
    }.withRetries();
  }

  public Map<String, String> unsetSessionVariables(final List<String> variables)  throws ServiceException {
    return new ServerCallable<Map<String, String>>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Map<String, String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        ClientProtos.UpdateSessionVariableRequest request = ClientProtos.UpdateSessionVariableRequest.newBuilder()
            .setSessionId(sessionId)
            .addAllUnsetVariables(variables).build();

        SessionUpdateResponse response = tajoMasterService.updateSessionVariables(null, request);

        if (response.getResultCode() == ResultCode.OK) {
          updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
          return Collections.unmodifiableMap(sessionVarsCache);
        } else {
          throw new ServiceException(response.getMessage());
        }
      }
    }.withRetries();
  }

  void updateSessionVarsCache(Map<String, String> variables) {
    synchronized (sessionVarsCache) {
      this.sessionVarsCache.clear();
      this.sessionVarsCache.putAll(variables);
    }
  }

  public String getSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<String>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public String call(NettyClientBase client) throws ServiceException {

        synchronized (sessionVarsCache) {
          // If a desired variable is client side one and exists in the cache, immediately return the variable.
          if (sessionVarsCache.containsKey(varname)) {
            return sessionVarsCache.get(varname);
          }
        }

        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.getSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Boolean existSessionVariable(final String varname) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.existSessionVariable(null, convertSessionedString(varname)).getValue();
      }
    }.withRetries();
  }

  public Map<String, String> getCachedAllSessionVariables() {
    synchronized (sessionVarsCache) {
      return Collections.unmodifiableMap(sessionVarsCache);
    }
  }

  public Map<String, String> getAllSessionVariables() throws ServiceException {
    return new ServerCallable<Map<String, String>>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class,
        false, true) {

      public Map<String, String> call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return ProtoUtil.convertToMap(tajoMasterService.getAllSessionVariables(null, sessionId));
      }
    }.withRetries();
  }

  public Boolean selectDatabase(final String databaseName) throws ServiceException {
    return new ServerCallable<Boolean>(connPool, getTajoMasterAddr(), TajoMasterClientProtocol.class, false, true) {

      public Boolean call(NettyClientBase client) throws ServiceException {
        checkSessionAndGet(client);

        TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
        return tajoMasterService.selectDatabase(null, convertSessionedString(databaseName)).getValue();
      }
    }.withRetries();
  }

  @Override
  public void close() {
    if(closed.getAndSet(true)){
      return;
    }

    // remove session
    try {

      NettyClientBase client = connPool.getConnection(getTajoMasterAddr(), TajoMasterClientProtocol.class, false);
      TajoMasterClientProtocolService.BlockingInterface tajoMaster = client.getStub();
      tajoMaster.removeSession(null, sessionId);

    } catch (Throwable e) {
    }

    if(connPool != null) {
      connPool.shutdown();
    }

    queryMasterMap.clear();
  }

  protected InetSocketAddress getTajoMasterAddr() {
    if (!conf.getBoolVar(TajoConf.ConfVars.TAJO_MASTER_HA_ENABLE)) {
      return tajoMasterAddr;
    } else {
      if (!HAServiceUtil.isMasterAlive(tajoMasterAddr, conf)) {
        return HAServiceUtil.getMasterClientAddress(conf);
      } else {
        return tajoMasterAddr;
      }
    }
  }

  protected void checkSessionAndGet(NettyClientBase client) throws ServiceException {

    if (sessionId == null) {

      TajoMasterClientProtocolService.BlockingInterface tajoMasterService = client.getStub();
      CreateSessionRequest.Builder builder = CreateSessionRequest.newBuilder();
      builder.setUsername(userInfo.getUserName()).build();

      if (baseDatabase != null) {
        builder.setBaseDatabaseName(baseDatabase);
      }

      CreateSessionResponse response = tajoMasterService.createSession(null, builder.build());

      if (response.getResultCode() == ResultCode.OK) {

        sessionId = response.getSessionId();
        updateSessionVarsCache(ProtoUtil.convertToMap(response.getSessionVars()));
        if (LOG.isDebugEnabled()) {
          LOG.debug(String.format("Got session %s as a user '%s'.", sessionId.getId(), userInfo.getUserName()));
        }

      } else {
        throw new InvalidClientSessionException(response.getMessage());
      }
    }
  }

  ClientProtos.SessionedStringProto convertSessionedString(String str) {
    ClientProtos.SessionedStringProto.Builder builder = ClientProtos.SessionedStringProto.newBuilder();
    builder.setSessionId(sessionId);
    builder.setValue(str);
    return builder.build();
  }

}
