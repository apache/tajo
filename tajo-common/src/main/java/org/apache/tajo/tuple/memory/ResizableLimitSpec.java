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

package org.apache.tajo.tuple.memory;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.FileUtil;

/**
 * It specifies the maximum size or increasing ratio. In addition,
 * it guarantees that all numbers are less than or equal to Integer.MAX_VALUE 2^31
 * due to ByteBuffer.
 */
public class ResizableLimitSpec {
  private final Log LOG = LogFactory.getLog(ResizableLimitSpec.class);

  public static final int MAX_SIZE_BYTES = Integer.MAX_VALUE;
  public static final ResizableLimitSpec DEFAULT_LIMIT = new ResizableLimitSpec(Integer.MAX_VALUE);

  private final long initSize;
  private final long limitBytes;
  private final float incRatio;
  private final float allowedOVerflowRatio;
  private final static float DEFAULT_ALLOWED_OVERFLOW_RATIO = 0.1f;
  private final static float DEFAULT_INCREASE_RATIO = 1.0f;

  public ResizableLimitSpec(long initSize) {
    this(initSize, MAX_SIZE_BYTES, DEFAULT_ALLOWED_OVERFLOW_RATIO);
  }

  public ResizableLimitSpec(long initSize, long limitBytes) {
    this(initSize, limitBytes, DEFAULT_ALLOWED_OVERFLOW_RATIO);
  }

  public ResizableLimitSpec(long initSize, long limitBytes, float allowedOverflow) {
    this(initSize, limitBytes, allowedOverflow, DEFAULT_INCREASE_RATIO);
  }

  public ResizableLimitSpec(long initSize, long limitBytes, float allowedOverflowRatio, float incRatio) {
    Preconditions.checkArgument(initSize > 0, "initial size must be greater than 0 bytes.");
    Preconditions.checkArgument(initSize <= MAX_SIZE_BYTES, "The maximum initial size is 2GB.");
    Preconditions.checkArgument(limitBytes > 0, "The limit size must be greater than 0 bytes.");
    Preconditions.checkArgument(limitBytes <= MAX_SIZE_BYTES, "The maximum limit size is 2GB.");
    Preconditions.checkArgument(incRatio > 0.0f, "Increase Ratio must be greater than 0.");

    if (initSize == limitBytes) {
      long overflowedSize = (long) (initSize + (initSize * allowedOverflowRatio));

      if (overflowedSize > Integer.MAX_VALUE) {
        overflowedSize = Integer.MAX_VALUE;
      }

      this.initSize = overflowedSize;
      this.limitBytes = overflowedSize;
    } else {
      this.initSize = initSize;
      limitBytes = (long) (limitBytes + (limitBytes * allowedOverflowRatio));

      if (limitBytes > Integer.MAX_VALUE) {
        this.limitBytes = Integer.MAX_VALUE;
      } else {
        this.limitBytes = limitBytes;
      }
    }

    this.allowedOVerflowRatio = allowedOverflowRatio;
    this.incRatio = incRatio;
  }

  public long initialSize() {
    return initSize;
  }

  public long limit() {
    return limitBytes;
  }

  public float remainRatio(long currentSize) {
    Preconditions.checkArgument(currentSize > 0, "Size must be greater than 0 bytes.");
    if (currentSize > Integer.MAX_VALUE) {
      currentSize = Integer.MAX_VALUE;
    }
    return (float)currentSize / (float)limitBytes;
  }

  public boolean canIncrease(long currentSize) {
    return remain(currentSize) > 0;
  }

  public long remain(long currentSize) {
    Preconditions.checkArgument(currentSize > 0, "Size must be greater than 0 bytes.");
    return limitBytes > Integer.MAX_VALUE ? Integer.MAX_VALUE - currentSize : limitBytes - currentSize;
  }

  public int increasedSize(int currentSize) {
    if (currentSize < initSize) {
      return (int) initSize;
    }

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
        + FileUtil.humanReadableByteCount(limitBytes, false) + ",overflow_ratio=" + allowedOVerflowRatio
        + ",inc_ratio=" + incRatio;
  }
}
