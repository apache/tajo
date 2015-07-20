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

import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.storage.Tuple;

import java.util.Comparator;

public class SetTupleComparator implements Comparator<Tuple> {
  private int numCompKey;

  private Datum outer;
  private Datum inner;
  private int compVal;

  public SetTupleComparator(Schema leftschema, Schema rightschema) {
    Preconditions.checkArgument(leftschema.size() == rightschema.size(),
        "The size of both side schema must be equals, but they are different: "
            + leftschema.size() + " and " + rightschema.size());

    this.numCompKey = leftschema.size(); // because it is guaranteed that the size of both schemas are the same
  }

  @Override
  public int compare(Tuple outerTuple, Tuple innerTuple) {
    for (int i = 0; i < numCompKey; i++) {
      outer = (outerTuple == null) ? NullDatum.get() : outerTuple.get(i);
      inner = (innerTuple == null) ? NullDatum.get() : innerTuple.get(i);

      if (outer.isNull()) {
        // NullDatum can handle comparison with all types of Datum
        compVal = outer.compareTo(inner);
      } else if(inner.isNull()) {
        // NullDatum is greater than any non NullDatums in Tajo
        compVal = -1;
      } else {
        // Both tuple are not NullDatum
        compVal = outer.compareTo(inner);
      }

      if (compVal != 0) {
        return compVal;
      }
    }
    return 0;
  }
}