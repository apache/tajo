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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BNLJoinExec extends CommonJoinExec {

  private final BufferedTuples leftTuples;
  private final BufferedTuples rightTuples;

  private final static int TUPLE_SLOT_SIZE = 10000;

  private Predicate<Tuple> predicate;

  public BNLJoinExec(final TaskAttemptContext context, final JoinNode plan,
                     final PhysicalExec leftExec, PhysicalExec rightExec) {
    super(context, plan, leftExec, rightExec);
    this.leftTuples = new BufferedTuples(leftExec, leftPredicate, TUPLE_SLOT_SIZE);
    this.rightTuples = new BufferedTuples(rightExec, rightPredicate, TUPLE_SLOT_SIZE);

    // for projection
    if (!plan.hasTargets()) {
      plan.setTargets(PlannerUtil.schemaToTargets(outSchema));
    }
  }

  @SuppressWarnings("unchecked")
  public void init() throws IOException {
    super.init();
    predicate = mergePredicates(toPredicate(equiQual), toPredicate(joinQual));
  }

  public Tuple next() throws IOException {
    while (!context.isStopped()) {
      if (!frameTuple.isSetLeft()) {
        if (!leftTuples.hasNext()) {
          return null;
        }
        frameTuple.setLeft(leftTuples.next());
      }
      while (rightTuples.hasNext()) {
        frameTuple.setRight(rightTuples.next());
        if (predicate == null || predicate.apply(frameTuple)) {
          projector.eval(frameTuple, outTuple);
          return outTuple;
        }
      }
      rightTuples.rescan();
      frameTuple.clear();
    }
    return null;
  }

  private static class BufferedTuples implements Iterator<Tuple> {

    private final List<Tuple> buffer;
    private final PhysicalExec source;
    private final Predicate<Tuple> predicate;

    private transient boolean eof;
    private transient Iterator<Tuple> buffered = Iterators.emptyIterator();

    private BufferedTuples(PhysicalExec source, Predicate<Tuple> predicate, int max) {
      this.buffer = new ArrayList<Tuple>(max);
      this.source = source;
      this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
      if (buffered.hasNext()) {
        return true;
      }
      try {
        buffered = fillBuffer();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return buffered.hasNext();
    }
    @Override
    public Tuple next() { return buffered.next(); }
    @Override
    public void remove() { throw new UnsupportedOperationException("remove"); }

    private Iterator<Tuple> fillBuffer() throws IOException {
      buffer.clear();
      while (!eof && buffer.size() < TUPLE_SLOT_SIZE) {
        Tuple t = source.next();
        if (isValid(t)) {
          buffer.add(t);
        }
        eof = t == null;
      }
      return buffer.iterator();
    }

    private boolean isValid(Tuple t) {
      return t != null && (predicate == null || predicate.apply(t));
    }

    public void rescan() throws IOException {
      eof = false;
      buffer.clear();
      buffered = Iterators.emptyIterator();
      source.rescan();
    }

    public void close() throws IOException {
      eof = true;
      buffer.clear();
      buffered = Iterators.emptyIterator();
    }
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();
    leftTuples.rescan();
    rightTuples.rescan();
  }

  @Override
  public void close() throws IOException {
    super.close();
    leftTuples.close();
    rightTuples.close();
  }
}
