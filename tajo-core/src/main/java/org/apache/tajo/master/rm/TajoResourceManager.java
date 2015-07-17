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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.scheduler.AbstractQueryScheduler;
import org.apache.tajo.master.scheduler.QuerySchedulingInfo;
import org.apache.tajo.master.scheduler.event.SchedulerEventType;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * It manages all resources of tajo workers.
 */
public class TajoResourceManager extends CompositeService {
  /** class logger */
  private static final Log LOG = LogFactory.getLog(TajoResourceManager.class);

  protected static final Map<String, Class<? extends AbstractQueryScheduler>> SCHEDULER_CLASS_CACHE = Maps.newHashMap();

  private TajoMaster.MasterContext masterContext;

  private TajoRMContext rmContext;

  private String queryIdSeed;

  /**
   * Node Liveliness monitor
   */
  private NodeLivelinessMonitor nodeLivelinessMonitor;

  private TajoConf systemConf;
  private AbstractQueryScheduler scheduler;

  /** It receives status messages from workers and their resources. */
  private TajoResourceTracker resourceTracker;

  public TajoResourceManager(TajoMaster.MasterContext masterContext) {
    super(TajoResourceManager.class.getSimpleName());
    this.masterContext = masterContext;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    this.systemConf = TUtil.checkTypeAndGet(conf, TajoConf.class);

    AsyncDispatcher dispatcher = new AsyncDispatcher();
    addIfService(dispatcher);

    rmContext = new TajoRMContext(dispatcher);

    this.queryIdSeed = String.valueOf(System.currentTimeMillis());

    this.nodeLivelinessMonitor = new NodeLivelinessMonitor(this.rmContext.getDispatcher());
    addIfService(this.nodeLivelinessMonitor);

    // Register event handler for Workers
    rmContext.getDispatcher().register(NodeEventType.class, new WorkerEventDispatcher(rmContext));

    resourceTracker = new TajoResourceTracker(this, nodeLivelinessMonitor);
    addIfService(resourceTracker);

    String schedulerClassName = systemConf.getVar(TajoConf.ConfVars.RESOURCE_SCHEDULER_CLASS);
    scheduler = loadScheduler(schedulerClassName);
    LOG.info("Loaded resource scheduler : " + scheduler.getClass());
    addIfService(scheduler);
    rmContext.getDispatcher().register(SchedulerEventType.class, scheduler);

    super.serviceInit(systemConf);
  }

  protected synchronized AbstractQueryScheduler loadScheduler(String schedulerClassName) throws Exception {
    Class<? extends AbstractQueryScheduler> schedulerClass;
    if (SCHEDULER_CLASS_CACHE.containsKey(schedulerClassName)) {
      schedulerClass = SCHEDULER_CLASS_CACHE.get(schedulerClassName);
    } else {
      schedulerClass = (Class<? extends AbstractQueryScheduler>) Class.forName(schedulerClassName);
      SCHEDULER_CLASS_CACHE.put(schedulerClassName, schedulerClass);
    }

    Constructor<? extends AbstractQueryScheduler>
        constructor = schedulerClass.getDeclaredConstructor(new Class[]{TajoMaster.MasterContext.class});
    constructor.setAccessible(true);

    return constructor.newInstance(new Object[]{masterContext});
  }

  @InterfaceAudience.Private
  public static final class WorkerEventDispatcher implements EventHandler<NodeEvent> {

    private final TajoRMContext rmContext;

    public WorkerEventDispatcher(TajoRMContext rmContext) {
      this.rmContext = rmContext;
    }

    @Override
    public void handle(NodeEvent event) {
      int workerId = event.getWorkerId();
      NodeStatus node = this.rmContext.getNodes().get(workerId);
      if (node != null) {
        try {
          node.handle(event);
        } catch (Throwable t) {
          LOG.error("Error in handling event type " + event.getType() + " for node " + workerId, t);
        }
      }
    }
  }


  public Map<Integer, NodeStatus> getNodes() {
    return ImmutableMap.copyOf(rmContext.getNodes());
  }

  public Map<Integer, NodeStatus> getInactiveNodes() {
    return ImmutableMap.copyOf(rmContext.getInactiveNodes());
  }

  public Collection<Integer> getQueryMasters() {
    return Collections.unmodifiableSet(rmContext.getQueryMasterWorker());
  }

  @Override
  public void serviceStop() throws Exception {
    super.serviceStop();
  }

  /**
   *
   * @return The prefix of queryId. It is generated when a TajoMaster starts up.
   */
  public String getSeedQueryId() throws IOException {
    return queryIdSeed;
  }

  @VisibleForTesting
  TajoResourceTracker getResourceTracker() {
    return resourceTracker;
  }

  public AbstractQueryScheduler getScheduler() {
    return scheduler;
  }

  public void submitQuery(QuerySchedulingInfo schedulingInfo) {
    scheduler.submitQuery(schedulingInfo);
  }

  public TajoRMContext getRMContext() {
    return rmContext;
  }
}
