/*
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

package tajo.master.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.service.AbstractService;
import org.apache.zookeeper.KeeperException;
import tajo.NConstants;
import tajo.master.cluster.ClusterManager.WorkerResource;
import tajo.master.cluster.event.WorkerEvent;
import tajo.master.cluster.event.WorkerEventType;
import tajo.zookeeper.ZkClient;
import tajo.zookeeper.ZkListener;
import tajo.zookeeper.ZkUtil;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkerTracker extends AbstractService implements ZkListener {
  private final Log LOG = LogFactory.getLog(WorkerTracker.class);
  private final ZkClient client;
  private Set<String> members = new HashSet<>();
  private final EventHandler eventHandler;
  private final Lock readLock;
  private final Lock writeLock;

  private final Map<InetSocketAddress, WorkerResource> workers
      = new ConcurrentHashMap<>();

  public WorkerTracker(ZkClient client, EventHandler eventHandler)
      throws Exception {
    super(WorkerTracker.class.getName());
    this.client = client;
    this.eventHandler = eventHandler;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
  }

  public void start()  {
    this.client.subscribe(this);

    try {
      writeLock.lock();

      List<String> nodeNames;

      nodeNames = ZkUtil
            .listChildrenAndWatchThem(this.client, NConstants.ZNODE_WORKERS);

      for (String nodeName : nodeNames) {
        this.members.add(nodeName);
      }

    } catch (Exception e) {
      LOG.error(e);

    } finally {
      writeLock.unlock();
    }

    super.start();
  }

  public void stop() {
    members.clear();
    super.stop();
  }

  @Override
  public void nodeCreated(String path) {
    // nothing
  }

  @Override
  public void nodeDeleted(String path) {
    if (path.startsWith(NConstants.ZNODE_WORKERS)) {
      String nodeName = ZkUtil.getNodeName(path);
      LOG.info("Worker ephemeral node deleted, processing expiration [" +
          nodeName + "]");

      try {
        writeLock.lock();
        if (members.contains(nodeName)) {
          members.remove(nodeName);
        } else {
          LOG.warn("Not registered worker:" + nodeName);
        }
      } finally {
        writeLock.unlock();
      }

      eventHandler.handle(new WorkerEvent(WorkerEventType.LEAVE,
          Lists.newArrayList(nodeName)));
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    // nothing
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(NConstants.ZNODE_WORKERS)) {

      Set<String> joined = null;
      Set<String> leaved = null;

      try {
        writeLock.lock();

        List<String> nodeNames =
            ZkUtil.listChildrenAndWatchThem(client, NConstants.ZNODE_WORKERS);

        Set<String> updated = new HashSet<>(nodeNames);
        joined = Sets.difference(updated, members).immutableCopy();
        leaved = Sets.difference(members, updated).immutableCopy();

        members.clear();
        members.addAll(updated);

      } catch (KeeperException e) {
        LOG.error(e.getMessage(), e);
      } finally {
        writeLock.unlock();
      }

      if (leaved.size() > 0) {
        eventHandler.handle(new WorkerEvent(WorkerEventType.LEAVE, leaved));
      }
      if (joined.size() > 0) {
        eventHandler.handle(new WorkerEvent(WorkerEventType.JOIN, joined));
      }

      for (String joinNode : joined) {
        LOG.info("Worker (" + joinNode + ") is joined");
      }

      for (String leavedNode : leaved) {
        LOG.info("Worker (" + leavedNode + ") is leaved");
      }
    }
  }

  public Collection<String> getMembers() {
    try {
      readLock.lock();

      return Collections.unmodifiableSet(members);

    } finally {
      readLock.unlock();
    }
  }
}
