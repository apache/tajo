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

package org.apache.tajo.engine.planner.logical.extended;

import com.google.common.collect.Lists;
import org.junit.Test;
import org.apache.tajo.engine.planner.logical.ExprType;

import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestReceiveNode {
  @Test
  public final void testReceiveNode() throws CloneNotSupportedException {
    ReceiveNode rec = new ReceiveNode(PipeType.PULL, RepartitionType.HASH);
    
    URI uri1 = URI.create("http://192.168.0.1:2190/?part=0");
    URI uri2 = URI.create("http://192.168.0.2:2190/?part=1");
    URI uri3 = URI.create("http://192.168.0.3:2190/?part=2");
    URI uri4 = URI.create("http://192.168.0.4:2190/?part=3");
    List<URI> set1 = Lists.newArrayList(uri1, uri2);
    List<URI> set2 = Lists.newArrayList(uri3, uri4);
    
    rec.addData("test1", set1.get(0));
    rec.addData("test1", set1.get(1));
    rec.addData("test2", set2.get(0));
    rec.addData("test2", set2.get(1));
    
    assertEquals(ExprType.RECEIVE, rec.getType());
    assertEquals(PipeType.PULL, rec.getPipeType());
    assertEquals(RepartitionType.HASH, rec.getRepartitionType());    
    assertEquals(set1, Lists.newArrayList(rec.getSrcURIs("test1")));
    assertEquals(set2, Lists.newArrayList(rec.getSrcURIs("test2")));
    
    ReceiveNode rec2 = (ReceiveNode) rec.clone();
    assertEquals(ExprType.RECEIVE, rec2.getType());
    assertEquals(PipeType.PULL, rec2.getPipeType());
    assertEquals(RepartitionType.HASH, rec2.getRepartitionType());    
    assertEquals(set1, Lists.newArrayList(rec2.getSrcURIs("test1")));
    assertEquals(set2, Lists.newArrayList(rec2.getSrcURIs("test2")));
    
    assertEquals(rec, rec2);
  }
}
