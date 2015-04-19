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

import org.apache.tajo.master.rm.TajoRMContext;

import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple Worker Id Generator for Tajo Worker.
 */
public class WorkerIdGenerator {
  private final TajoRMContext rmContext;

  private static final AtomicInteger connectionInfoIdGenerator =
      new AtomicInteger(0);

  private static final AtomicLong randomSeed =
      new AtomicLong(392994904958l);

  private final SecureRandom random;

  public WorkerIdGenerator(TajoRMContext rmContext) {
    random = new SecureRandom(generateRandomSeed());
    this.rmContext = rmContext;
  }

  public int getGeneratedId() {
    int localId;

    do {
      localId = connectionInfoIdGenerator.addAndGet(random.nextInt(4)+1);
    } while (
        rmContext.getWorkers().containsKey(localId) || rmContext.getInactiveWorkers().containsKey(localId)
        || localId == WorkerConnectionInfo.UNALLOCATED_WORKER_ID);

    return localId;
  }

  /**
   * Generate random seed by using prime numbers
   *
   * @return
   */
  private byte[] generateRandomSeed() {
    long localSeed, currentSeed;
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
}
