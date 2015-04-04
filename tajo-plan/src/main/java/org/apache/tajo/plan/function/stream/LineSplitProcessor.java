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

package org.apache.tajo.plan.function.stream;

import io.netty.buffer.ByteBufProcessor;

public class LineSplitProcessor implements ByteBufProcessor {
  public static final byte CR = '\r';
  public static final byte LF = '\n';
  private boolean prevCharCR = false; //true of prev char was CR

  @Override
  public boolean process(byte value) throws Exception {
    switch (value) {
      case LF:
        return false;
      case CR:
        prevCharCR = true;
        return false;
      default:
        prevCharCR = false;
        return true;
    }
  }

  public boolean isPrevCharCR() {
    return prevCharCR;
  }
}
