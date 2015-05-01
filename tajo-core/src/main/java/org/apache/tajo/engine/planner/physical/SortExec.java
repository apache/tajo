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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.storage.BaseTupleComparator;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

public abstract class SortExec extends UnaryPhysicalExec {

  protected final BaseTupleComparator comparator;
  protected final SortSpec [] sortSpecs;

  public SortExec(TaskAttemptContext context, Schema inSchema,
                  Schema outSchema, PhysicalExec child, SortSpec [] sortSpecs) {
    super(context, inSchema, outSchema, child);
    this.sortSpecs = sortSpecs;
    this.comparator = new BaseTupleComparator(inSchema, sortSpecs);
  }

  protected TupleSorter getSorter(List<Tuple> tupleSlots) {
    if (!tupleSlots.isEmpty() && ComparableVector.isVectorizable(sortSpecs)) {
      return new VectorizedSorter(tupleSlots, sortSpecs, comparator.getSortKeyIds());
    }
    return new TupleSorter.DefaultSorter(tupleSlots, comparator);
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  public Comparator<Tuple> getComparator() {
    return comparator;
  }

  @Override
  abstract public Tuple next() throws IOException;
}
