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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.worker.TaskAttemptContext;
import org.apache.tajo.engine.planner.logical.SortNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class MemSortExec extends SortExec {
  private SortNode plan;
  private List<Tuple> tupleSlots;
  private boolean sorted = false;
  private Iterator<Tuple> iterator;
  
  public MemSortExec(final TaskAttemptContext context,
                     SortNode plan, PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child, plan.getSortKeys());
    this.plan = plan;
  }

  public void init() throws IOException {
    super.init();
    this.tupleSlots = new ArrayList<Tuple>(1000);
  }

  @Override
  public Tuple next() throws IOException {

    if (!sorted) {
      Tuple tuple;
      while ((tuple = child.next()) != null) {
        tupleSlots.add(new VTuple(tuple));
      }
      
      Collections.sort(tupleSlots, getComparator());
      this.iterator = tupleSlots.iterator();
      sorted = true;
    }
    
    if (iterator.hasNext()) {
      return this.iterator.next();
    } else {
      return null;
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    this.iterator = tupleSlots.iterator();
    sorted = true;
  }

  @Override
  public void close() throws IOException {
    super.close();
    tupleSlots.clear();
    tupleSlots = null;
    iterator = null;
    plan = null;
  }

  public SortNode getPlan() {
    return this.plan;
  }
}
