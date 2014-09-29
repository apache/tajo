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

package org.apache.tajo.engine.codegen;

import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.storage.BaseTupleComparator;

public class CompilationError extends RuntimeException {
  public CompilationError(String message) {
    super(message);
  }

  public CompilationError(EvalNode evalNode, Throwable t, byte [] clazz) {
    super(t.getMessage() +
        "\nCompilation Error: " + evalNode.toString() + "\n\nBYTES CODE DUMP:\n" + CodeGenUtils.disassemble(clazz), t);
  }

  public CompilationError(BaseTupleComparator comp, Throwable t, byte [] clazz) {
    super(t.getMessage() +
        "\nCompilation Error: " + comp.toString() + "\n\nBYTES CODE DUMP:\n" + CodeGenUtils.disassemble(clazz), t);
  }
}
