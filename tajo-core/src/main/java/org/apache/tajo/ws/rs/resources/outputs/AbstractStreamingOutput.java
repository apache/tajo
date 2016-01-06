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

package org.apache.tajo.ws.rs.resources.outputs;

import org.apache.tajo.master.exec.NonForwardQueryResultScanner;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;

public abstract class AbstractStreamingOutput implements StreamingOutput {
  protected NonForwardQueryResultScanner scanner;
  protected int count;
  protected int startOffset;

  public AbstractStreamingOutput(NonForwardQueryResultScanner scanner, Integer count, Integer startoffset) {
    this.scanner = scanner;
    this.count = count;
    this.startOffset = startoffset;
  }

  public abstract boolean hasLength();
  public abstract int count();
  public abstract int length();

  public String contentType() {
    return MediaType.APPLICATION_OCTET_STREAM;
  }
}
