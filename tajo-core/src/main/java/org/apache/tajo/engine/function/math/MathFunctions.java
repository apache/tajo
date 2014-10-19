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

package org.apache.tajo.engine.function.math;

import org.apache.tajo.function.FunctionCollection;
import org.apache.tajo.function.ScalarFunction;

import static org.apache.tajo.common.TajoDataTypes.Type.FLOAT8;

@FunctionCollection
public class MathFunctions {

  @ScalarFunction(name = "pi", returnType = FLOAT8)
  public static double pi() {
    return Math.PI;
  }

  @ScalarFunction(name = "pow", returnType = FLOAT8, paramTypes = {FLOAT8, FLOAT8})
  public static Double pow(Double x, Double y) {
    if (x == null || y == null) {
      return null;
    }
    return Math.pow(x, y);
  }

//  @ScalarFunction(name = "pow", returnType = FLOAT8, paramTypes = {FLOAT8, FLOAT8})
//  public static double pow(double x, double y) {
//    return Math.pow(x, y);
//  }
}
