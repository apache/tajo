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

package org.apache.tajo.thrift.client;

import org.apache.tajo.rpc.RemoteException;
import org.apache.tajo.thrift.generated.TServiceException;
import org.apache.tajo.thrift.generated.TajoThriftService.Client;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;

public abstract class ThriftServerCallable<T> {
  protected long startTime;
  protected long endTime;
  protected boolean abort;
  protected Client client;

  public abstract T call(Client client) throws Exception;

  public ThriftServerCallable(Client client) {
    this.client = client;
  }

  public void abort() {
    abort = true;
  }

  protected void beforeCall() {
    this.startTime = System.currentTimeMillis();
  }

  public long getStartTime(){
    return startTime;
  }

  protected void failedCall() throws Exception {}

  protected void afterCall() {
    this.endTime = System.currentTimeMillis();
  }

  public long getEndTime(){
    return endTime;
  }

  /**
   * Run this instance with retries, timed waits,
   * and refinds of missing regions.
   *
   * @param <T> the type of the return value
   * @return an object of type T
   * @throws TException if a remote or network exception occurs
   */
  public T withRetries() throws TException {
    final long pause = 500; //ms
    final int numRetries = 3;
    List<Throwable> exceptions = new ArrayList<Throwable>();

    for (int tries = 0; tries < numRetries; tries++) {
      try {
        beforeCall();
        return call(client);
      } catch (TServiceException se) {
        // Failed while running business logic
        throw new TException(se.getMessage(), se);
      } catch (Throwable t) {
        // Failed while networking
        if(abort) {
          throw new TException(t.getMessage(), t);
        }
        exceptions.add(t);
        if (tries == numRetries - 1) {
          throw new TException("Giving up after tries=" + tries, t);
        }
        try {
          failedCall();
        } catch (Exception e) {
          throw new TException("Error occurs while call failedCall(), last error=" + t.getMessage() +
              ", reconnection error=" + e.getMessage(), e);
        }
      } finally {
        afterCall();
      }
      try {
        Thread.sleep(pause * (tries + 1));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new TException("Giving up after tries=" + tries, e);
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
    try {
      beforeCall();
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
