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

package org.apache.tajo.rpc;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ServiceException;
import org.apache.tajo.conf.TajoConf;

public abstract class ServerCallable<T> {
  protected InetSocketAddress addr;
  protected long startTime;
  protected long endTime;
  protected Class protocol;
  protected boolean asyncMode;
  protected boolean closeConn;
  protected RpcConnectionPool connPool;

  public abstract T call(NettyClientBase client) throws Exception;

  public ServerCallable(RpcConnectionPool connPool,  InetSocketAddress addr, Class protocol, boolean asyncMode) {
    this(connPool, addr, protocol, asyncMode, false);
  }

  public ServerCallable(RpcConnectionPool connPool, InetSocketAddress addr, Class protocol,
                        boolean asyncMode, boolean closeConn) {
    this.connPool = connPool;
    this.addr = addr;
    this.protocol = protocol;
    this.asyncMode = asyncMode;
    this.closeConn = closeConn;
  }

  public void beforeCall() {
    this.startTime = System.currentTimeMillis();
  }

  public long getStartTime(){
    return startTime;
  }

  public void afterCall() {
    this.endTime = System.currentTimeMillis();
  }

  public long getEndTime(){
    return endTime;
  }

  boolean abort = false;
  public void abort() {
    abort = true;
  }
  /**
   * Run this instance with retries, timed waits,
   * and refinds of missing regions.
   *
   * @param <T> the type of the return value
   * @return an object of type T
   * @throws com.google.protobuf.ServiceException if a remote or network exception occurs
   */
  public T withRetries() throws ServiceException {
    //TODO configurable
    final long pause = 500; //ms
    final int numRetries = 3;
    List<Throwable> exceptions = new ArrayList<Throwable>();

    for (int tries = 0; tries < numRetries; tries++) {
      NettyClientBase client = null;
      try {
        beforeCall();
        if(addr != null) {
          client = connPool.getConnection(addr, protocol, asyncMode);
        }
        return call(client);
      } catch (IOException ioe) {
        exceptions.add(ioe);
        if(abort) {
          throw new ServiceException(ioe.getMessage(), ioe);
        }
        if (tries == numRetries - 1) {
          throw new ServiceException("Giving up after tries=" + tries, ioe);
        }
      } catch (Throwable t) {
        throw new ServiceException(t);
      } finally {
        afterCall();
        if(closeConn) {
          connPool.closeConnection(client);
        } else {
          connPool.releaseConnection(client);
        }
      }
      try {
        Thread.sleep(pause * (tries + 1));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new ServiceException("Giving up after tries=" + tries, e);
      }
    }
    return null;
  }

  /**
   * Run this instance against the server once.
   * @param <T> the type of the return value
   * @return an object of type T
   * @throws java.io.IOException if a remote or network exception occurs
   * @throws RuntimeException other unspecified error
   */
  public T withoutRetries() throws IOException, RuntimeException {
    NettyClientBase client = null;
    try {
      beforeCall();
      client = connPool.getConnection(addr, protocol, asyncMode);
      return call(client);
    } catch (Throwable t) {
      Throwable t2 = translateException(t);
      if (t2 instanceof IOException) {
        throw (IOException)t2;
      } else {
        throw new RuntimeException(t2);
      }
    } finally {
      afterCall();
      if(closeConn) {
        connPool.closeConnection(client);
      } else {
        connPool.releaseConnection(client);
      }
    }
  }

  private static Throwable translateException(Throwable t) throws IOException {
    if (t instanceof UndeclaredThrowableException) {
      t = t.getCause();
    }
    if (t instanceof RemoteException && t.getCause() != null) {
      t = t.getCause();
    }
    return t;
  }
}
