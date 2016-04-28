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

package org.apache.tajo.plan.expr;

import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.type.Type;

public class NotEval extends UnaryEval implements Cloneable {
  public NotEval(EvalNode child) {
    super(EvalType.NOT, child);
  }

  @Override
  public Type getValueType() {
    return Type.Bool;
  }

  @Override
  public String getName() {
    return "?";
  }

  @Override
  @SuppressWarnings("unchecked")
  public Datum eval(Tuple tuple) {
    super.eval(tuple);
    Datum datum = child.eval(tuple);
    return !datum.isNull() ? DatumFactory.createBool(!datum.asBool()) : datum;
  }

  @Override
  public String toString() {
    return "NOT " + child.toString();
  }
}
