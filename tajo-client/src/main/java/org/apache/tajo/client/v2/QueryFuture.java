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

package org.apache.tajo.client.v2;

import org.apache.tajo.auth.UserRoleInfo;

import java.sql.ResultSet;
import java.util.concurrent.Future;

/**
 *
 */
public interface QueryFuture extends Future<ResultSet>, AutoCloseable {
  /**
   * Get a query id
   *
   * @return query id
   */
  String id();

  /**
   * Get the queue name that the query is running
   *
   * @return queue name
   */
  String queue();

  /**
   * Get a query state
   *
   * @return query state
   */
  QueryState state();

  /**
   * Get a normalized progress (0 ~ 1.0f) of a query running
   *
   * @return progress
   */
  float progress();

  /**
   * A submitted or running query state is normal
   *
   * @return True if a query state is normal
   */
  boolean isOk();

  /**
   * Get whether the query is successfully completed or not.
   *
   * @return True if the query is successfully completed.
   */
  boolean isSuccessful();

  /**
   * Get whether the query is abort due to error.
   *
   * @return True if the query is abort due to error.
   */
  boolean isFailed();

  /**
   * Get whether the query is killed. This is equivalent to
   * @{link Future#cancel}.
   *
   * @return True if the query is already killed.
   */
  boolean isKilled();

  /**
   * Get an user information
   *
   * @return UserRoleInfo
   */
  UserRoleInfo user();

  /**
   * Kill this query
   */
  void kill();

  /**
   * Get the time when a query is submitted.
   * This time can be different from @{link QueryFuture#startTime}
   * due to scheduling delay.
   *
   * @return Millisecond since epoch
   */
  long submitTime();

  /**
   * Get the time when a query is actually launched.
   *
   * @return Millisecond since epoch
   */
  long startTime();

  /**
   * Get the time when a query is finished.
   *
   * @return Millisecond since epoch
   */
  long finishTime();

  /**
   * Release a query future. It will be automatically released after the session invalidation.
   */
  void close();

  /**
   * Add a listener which will be executed after this query is completed, error, failed or killed.
   *
   * @param future
   */
  void addListener(FutureListener<QueryFuture> future);
}
