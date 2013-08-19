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

package org.apache.tajo.engine.planner.logical;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class TestSendNode {
  @Test
  public final void testSendNode() throws CloneNotSupportedException {
    SendNode send = new SendNode(PipeType.PULL, RepartitionType.HASH);
    send.putDestURI(0, URI.create("http://localhost:2190"));
    send.putDestURI(1, URI.create("http://localhost:2191"));
    
    assertEquals(NodeType.SEND, send.getType());
    assertEquals(PipeType.PULL, send.getPipeType());
    assertEquals(RepartitionType.HASH, send.getRepartitionType());
    assertEquals(URI.create("http://localhost:2190"), send.getDestURI(0));
    assertEquals(URI.create("http://localhost:2191"), send.getDestURI(1));
    
    SendNode send2 = (SendNode) send.clone();
    assertEquals(NodeType.SEND, send2.getType());
    assertEquals(PipeType.PULL, send2.getPipeType());
    assertEquals(RepartitionType.HASH, send2.getRepartitionType());
    assertEquals(URI.create("http://localhost:2190"), send2.getDestURI(0));
    assertEquals(URI.create("http://localhost:2191"), send2.getDestURI(1));
    
    assertEquals(send, send2);
  }
}
