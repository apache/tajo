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

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.plan.InvalidQueryException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Arrays;

public class SortIntersectExec extends BinaryPhysicalExec {
  SetTupleComparator comparator;
  Tuple lastReturned = null;
  boolean isDistinct = false;
  public SortIntersectExec(TaskAttemptContext context, PhysicalExec left, PhysicalExec right, boolean isDistinct) {
    super(context, left.getSchema(), right.getSchema(), left, right);
    TajoDataTypes.DataType[] leftTypes = SchemaUtil.toDataTypes(left.getSchema());
    TajoDataTypes.DataType[] rightTypes = SchemaUtil.toDataTypes(right.getSchema());
    if (!CatalogUtil.isMatchedFunction(Arrays.asList(leftTypes), Arrays.asList(rightTypes))) {
      throw new InvalidQueryException(
          "The both schemas are not compatible");
    }
    comparator = new SetTupleComparator(left.getSchema(), right.getSchema());
    this.isDistinct = isDistinct;
  }

  @Override
  public Tuple next() throws IOException {
    if (!context.isStopped()) {
      Tuple leftTuple = leftChild.next();
      Tuple rightTuple = rightChild.next();
      if (leftTuple == null || rightTuple == null) {
        return null;
      }

      // handling routine for INTERSECT without ALL
      // it eliminates duplicated return of the same row values
      if (isDistinct && lastReturned != null) {
        while (comparator.compare(leftTuple, lastReturned) == 0) {
          leftTuple = leftChild.next();
          if (leftTuple == null)
            return null;
        }
      }

      // At this point, Both Tuples are not null
      do {
        int compVal = comparator.compare(leftTuple, rightTuple);

        if (compVal > 0) {
          rightTuple = rightChild.next();
        } else if (compVal < 0) {
          leftTuple = leftChild.next();
        } else {
          lastReturned = leftTuple;
          return leftTuple;
        }
      } while (leftTuple != null && rightTuple != null);

      return null;
    }
    return null;
  }

  @Override
  public void rescan() throws IOException {
    super.rescan();

    lastReturned = null;
  }
}
