/***
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.exception.UnimplementedException;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.tuple.offheap.OffHeapRowBlock;

import java.io.IOException;
import java.util.Comparator;

/**
 * Two-way merger scanner that reads two input sources and outputs one output tuples sorted in some order.
 */
class PairWiseMerger implements Scanner {
  private static final Log LOG = LogFactory.getLog(PairWiseMerger.class);

  private Scanner leftScan;
  private Scanner rightScan;

  private VTuple outTuple;
  private VTuple leftTuple;
  private VTuple rightTuple;

  private final Schema schema;
  private final Comparator<Tuple> comparator;

  private float mergerProgress;
  private TableStats mergerInputStats;

  private static enum State {
    NEW,
    INITED,
    CLOSED
  }

  private State state = State.NEW;

  public PairWiseMerger(Schema schema, Scanner leftScanner, Scanner rightScanner, Comparator<Tuple> comparator)
      throws IOException {
    this.schema = schema;
    this.leftScan = leftScanner;
    this.rightScan = rightScanner;
    this.comparator = comparator;
  }

  private void setState(State state) {
    this.state = state;
  }

  @Override
  public void init() throws IOException {
    if (state == State.NEW) {
      leftScan.init();
      rightScan.init();

      prepareTuplesForFirstComparison();

      mergerInputStats = new TableStats();
      mergerProgress = 0.0f;

      setState(State.INITED);
    } else {
      throw new IllegalStateException("Illegal State: init() is not allowed in " + state.name());
    }
  }

  private void prepareTuplesForFirstComparison() throws IOException {
    Tuple lt = leftScan.next();
    if (lt != null) {
      leftTuple = new VTuple(lt);
    } else {
      leftTuple = null; // TODO - missed free
    }

    Tuple rt = rightScan.next();
    if (rt != null) {
      rightTuple = new VTuple(rt);
    } else {
      rightTuple = null; // TODO - missed free
    }
  }

  public Tuple next() throws IOException {

    if (leftTuple != null && rightTuple != null) {
      if (comparator.compare(leftTuple, rightTuple) < 0) {
        outTuple = new VTuple(leftTuple);

        Tuple lt = leftScan.next();
        if (lt != null) {
          leftTuple = new VTuple(lt);
        } else {
          leftTuple = null; // TODO - missed free
        }
      } else {
        outTuple = new VTuple(rightTuple);

        Tuple rt = rightScan.next();
        if (rt != null) {
          rightTuple = new VTuple(rt);
        } else {
          rightTuple = null; // TODO - missed free
        }
      }
      return outTuple;
    }

    if (leftTuple == null) {
      if (rightTuple != null) {
        outTuple = new VTuple(rightTuple);
      } else {
        outTuple = null;
      }

      Tuple rt = rightScan.next();
      if (rt != null) {
        rightTuple = new VTuple(rt);
      } else {
        rightTuple = null; // TODO - missed free
      }
    } else {
      if (leftTuple != null) {
        outTuple = new VTuple(leftTuple);
      } else {
        outTuple = null;
      }

      Tuple lt = leftScan.next();
      if (lt != null) {
        leftTuple = new VTuple(lt);
      } else {
        leftTuple = null; // TODO - missed free
      }
    }
    return outTuple;
  }

  @Override
  public boolean nextFetch(OffHeapRowBlock rowBlock) {
    throw new UnimplementedException("nextFetch(OffHeapRowBlock) is not implemented");
  }

  @Override
  public void reset() throws IOException {
    if (state == State.INITED) {
      leftScan.reset();
      rightScan.reset();

      outTuple = null;
      leftTuple = null;
      rightTuple = null;

      prepareTuplesForFirstComparison();
    } else {
      throw new IllegalStateException("Illegal State: init() is not allowed in " + state.name());
    }
  }

  public void close() throws IOException {
    IOUtils.cleanup(PairWiseMerger.LOG, leftScan, rightScan);
    getInputStats();
    leftScan = null;
    rightScan = null;
    mergerProgress = 1.0f;
    setState(State.CLOSED);
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setSearchCondition(Object expr) {
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public float getProgress() {
    if (leftScan == null) {
      return mergerProgress;
    }
    return leftScan.getProgress() * 0.5f + rightScan.getProgress() * 0.5f;
  }

  @Override
  public TableStats getInputStats() {
    if (leftScan == null) {
      return mergerInputStats;
    }
    TableStats leftInputStats = leftScan.getInputStats();
    if (mergerInputStats == null) {
      mergerInputStats = new TableStats();
    }
    mergerInputStats.setNumBytes(0);
    mergerInputStats.setReadBytes(0);
    mergerInputStats.setNumRows(0);

    if (leftInputStats != null) {
      mergerInputStats.setNumBytes(leftInputStats.getNumBytes());
      mergerInputStats.setReadBytes(leftInputStats.getReadBytes());
      mergerInputStats.setNumRows(leftInputStats.getNumRows());
    }

    TableStats rightInputStats = rightScan.getInputStats();
    if (rightInputStats != null) {
      mergerInputStats.setNumBytes(mergerInputStats.getNumBytes() + rightInputStats.getNumBytes());
      mergerInputStats.setReadBytes(mergerInputStats.getReadBytes() + rightInputStats.getReadBytes());
      mergerInputStats.setNumRows(mergerInputStats.getNumRows() + rightInputStats.getNumRows());
    }

    return mergerInputStats;
  }
}
