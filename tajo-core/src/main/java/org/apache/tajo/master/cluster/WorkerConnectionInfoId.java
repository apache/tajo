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

package org.apache.tajo.master.cluster;

import java.security.SecureRandom;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Provides simple unique integer id generator
 * 
 */
public class WorkerConnectionInfoId {

  private static final ConcurrentSkipListSet<Integer> allocatedIdSet =
      new ConcurrentSkipListSet<Integer>();
  
  private static final AtomicInteger connectionInfoIdGenerator =
      new AtomicInteger(0);
  
  private static final AtomicLong randomSeed =
      new AtomicLong(392994904958l);
  
  private SecureRandom random;
  
  private final int id;
  
  public WorkerConnectionInfoId() {
    this.id = getGeneratedId();
  }
  
  public WorkerConnectionInfoId(int id) {
    this.id = id;
    
    // This constructor does not check whether this id is unique or not.
    // However, this id will be put into generated id set for guaranteeing uniqueness of new instance.
    allocatedIdSet.add(this.id);
  }
  
  private int getGeneratedId() {
    random = new SecureRandom(generateRandomSeed());
    int localId = 0;
    
    do {
      localId = connectionInfoIdGenerator.addAndGet(random.nextInt(4)+1);
    } while (!allocatedIdSet.add(localId));
    
    return localId;
  }
  
  /**
   * Generate random seed by using prime numbers
   * 
   * @return
   */
  private byte[] generateRandomSeed() {
    long localSeed = 0, currentSeed = 0;
    byte[] localSeedBytes = new byte[8];
    
    do {
      currentSeed = randomSeed.get();
      localSeed = (currentSeed * 4294967291l) ^ 0x16646CA9;
    } while (!randomSeed.compareAndSet(currentSeed, localSeed));
    
    for (int byteIdx = 0; byteIdx < localSeedBytes.length; byteIdx++) {
      localSeedBytes[byteIdx] = (byte) ((localSeed >> 8) & 0xFF);
    }
    
    return localSeedBytes;
  }
  
  public int getId() {
    return id;
  }

  @Override
  protected void finalize() throws Throwable {
    allocatedIdSet.remove(this.id);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    WorkerConnectionInfoId other = (WorkerConnectionInfoId) obj;
    if (id != other.id)
      return false;
    return true;
  }
  
}
