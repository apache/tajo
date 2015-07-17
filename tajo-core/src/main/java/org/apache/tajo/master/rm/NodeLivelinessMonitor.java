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

package org.apache.tajo.master.rm;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tajo.conf.TajoConf;

/**
 * It periodically checks the latest heartbeat time of {@link NodeStatus}.
 * If the latest heartbeat time is expired, it produces EXPIRE event to a corresponding {@link NodeStatus}.
 */
public class NodeLivelinessMonitor extends AbstractLivelinessMonitor<Integer> {

  private EventHandler dispatcher;

  public NodeLivelinessMonitor(Dispatcher d) {
    super(NodeLivelinessMonitor.class.getSimpleName(), new SystemClock());
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    Preconditions.checkArgument(conf instanceof TajoConf);
    TajoConf systemConf = (TajoConf) conf;
    // milliseconds
    int expireIntvl = systemConf.getIntVar(TajoConf.ConfVars.RESOURCE_TRACKER_HEARTBEAT_TIMEOUT) * 1000;
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(Integer id) {
    dispatcher.handle(new NodeEvent(id, NodeEventType.EXPIRE));
  }
}
