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

/**
 * A user event triggered by {@link org.apache.tajo.rpc.MonitorStateEvent} when a ping packet is not receive.
 */
public final class MonitorStateEvent {
  /**
   * An {@link Enum} that represents the monitor state of a remote peer.
   */
  public enum MonitorState {
    /**
     * No ping was received.
     */
    PING_EXPIRED
  }

  public static final MonitorStateEvent MONITOR_EXPIRED_STATE_EVENT = new MonitorStateEvent(MonitorState.PING_EXPIRED);

  private final MonitorState state;

  private MonitorStateEvent(MonitorState state) {
    this.state = state;
  }

  /**
   * Returns the monitor state.
   */
  public MonitorState state() {
    return state;
  }
}