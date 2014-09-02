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

package org.apache.tajo.storage.offheap;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.FileUtil;

public class ResizableSpec {
  private final Log LOG = LogFactory.getLog(ResizableSpec.class);

  public static final int MAX_SIZE_BYTES = Integer.MAX_VALUE;
  public static final ResizableSpec DEFAULT_LIMIT = new ResizableSpec(Integer.MAX_VALUE);

  private final int initSize;
  private final int limitBytes;
  private final float incRatio;
  private final static float DEFAULT_INCREASE_RATIO = 1.0f;

  public ResizableSpec(int initSize) {
    this(initSize, MAX_SIZE_BYTES);
  }

  public ResizableSpec(int initSize, int limitBytes) {
    this(initSize, limitBytes, DEFAULT_INCREASE_RATIO);
  }

  public ResizableSpec(int initSize, int limitBytes, float incRatio) {
    Preconditions.checkArgument(initSize > 0, "initial size must be greater than 0 bytes.");
    Preconditions.checkArgument(initSize <= MAX_SIZE_BYTES, "The maximum initial size is 2GB.");
    Preconditions.checkArgument(limitBytes > 0, "The limit size must be greater than 0 bytes.");
    Preconditions.checkArgument(limitBytes <= MAX_SIZE_BYTES, "The maximum limit size is 2GB.");
    Preconditions.checkArgument(incRatio > 0.0f, "Increase Ratio must be greater than 0.");

    this.initSize = initSize;
    this.limitBytes = limitBytes;
    this.incRatio = incRatio;
  }

  public int initialSize() {
    return initSize;
  }

  public int limit() {
    return limitBytes;
  }

  public float remainRatio(int currentSize) {
    return (float)currentSize / (float)limitBytes;
  }

  public float increaseRatio() {
    return incRatio;
  }

  public boolean canIncrease(int currentSize) {
    return remain(currentSize) > 0;
  }

  public long remain(int currentSize) {
    return limitBytes > Integer.MAX_VALUE ? Integer.MAX_VALUE - currentSize : limitBytes - currentSize;
  }

  public int increasedSize(int currentSize) {
    if (currentSize > Integer.MAX_VALUE) {
      LOG.warn("Current size already exceeds the maximum size (" + Integer.MAX_VALUE + " bytes)");
      return Integer.MAX_VALUE;
    }
    long nextSize = (long) (currentSize + ((float) currentSize * incRatio));

    if (nextSize > limitBytes) {
      LOG.info("Increasing reaches size limit (" + FileUtil.humanReadableByteCount(limitBytes, false) + ")");
      nextSize = limitBytes;
    }

    if (nextSize > Integer.MAX_VALUE) {
      LOG.info("Increasing reaches maximum size (" + FileUtil.humanReadableByteCount(Integer.MAX_VALUE, false) + ")");
      nextSize = Integer.MAX_VALUE;
    }

    return (int) nextSize;
  }

  @Override
  public String toString() {
    return "init=" + FileUtil.humanReadableByteCount(initSize, false) + ",limit="
        + FileUtil.humanReadableByteCount(limitBytes, false) + ",incRatio=" + incRatio;
  }
}
