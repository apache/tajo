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

package org.apache.tajo.master.scheduler;

import java.lang.String;import java.util.List;

/**
 * QueueInfo is a report of the runtime information of the queue.
 * <p>
 * It includes information such as:
 * <ul>
 *   <li>Queue name.</li>
 *   <li>Capacity of the queue.</li>
 *   <li>Maximum capacity of the queue.</li>
 *   <li>Current capacity of the queue.</li>
 *   <li>Child queues.</li>
 *   <li>Running applications.</li>
 *   <li>{@link QueueState} of the queue.</li>
 * </ul>
 *
 */

public abstract class QueueInfo {
  /**
   * Get the <em>name</em> of the queue.
   * @return <em>name</em> of the queue
   */
  public abstract String getQueueName();

  public abstract void setQueueName(String queueName);

  /**
   * Get the <em>configured capacity</em> of the queue.
   * @return <em>configured capacity</em> of the queue
   */
  public abstract float getCapacity();

  public abstract void setCapacity(float capacity);
  
  /**
   * Get the <em>maximum capacity</em> of the queue.
   * @return <em>maximum capacity</em> of the queue
   */

  public abstract float getMaximumCapacity();

  public abstract void setMaximumCapacity(float maximumCapacity);

  /**
   * Get the <em>maximum query capacity</em> of the queue.
   * @return <em>maximum query capacity</em> of the queue
   */

  public abstract float getMaximumQueryCapacity();

  public abstract void setMaximumQueryCapacity(float maximumQueryCapacity);

  /**
   * Get the <em>current capacity</em> of the queue.
   * @return <em>current capacity</em> of the queue
   */

  public abstract float getCurrentCapacity();

  public abstract void setCurrentCapacity(float currentCapacity);
  
  /**
   * Get the <em>child queues</em> of the queue.
   * @return <em>child queues</em> of the queue
   */

  public abstract List<QueueInfo> getChildQueues();

  public abstract void setChildQueues(List<QueueInfo> childQueues);

  
  /**
   * Get the <code>QueueState</code> of the queue.
   * @return <code>QueueState</code> of the queue
   */
  public abstract QueueState getQueueState();

  public abstract void setQueueState(QueueState queueState);

}
