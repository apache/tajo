/*
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

import com.google.common.collect.Maps;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.org.objectweb.asm.Label;

import java.util.NavigableMap;
import java.util.Stack;

public class CaseWhenSwitchGenerator implements TajoGeneratorAdapter.SwitchCaseGenerator {
  final private EvalCodeGenerator generator;
  final private EvalCodeGenContext context;
  final private Stack<EvalNode> stack;

  final NavigableMap<Integer, TajoGeneratorAdapter.SwitchCase> casesMap;
  final EvalNode defaultEval;

  public CaseWhenSwitchGenerator(EvalCodeGenerator generator, EvalCodeGenContext context, Stack<EvalNode> stack,
                                 TajoGeneratorAdapter.SwitchCase[] cases, EvalNode defaultEval) {
    this.generator = generator;
    this.context = context;
    this.stack = stack;
    this.casesMap = Maps.newTreeMap();
    for (TajoGeneratorAdapter.SwitchCase switchCase : cases) {
      this.casesMap.put(switchCase.key(), switchCase);
    }
    this.defaultEval = defaultEval;
  }

  @Override
  public int size() {
    return casesMap.size();
  }

  @Override
  public int min() {
    return casesMap.firstEntry().getKey();
  }

  @Override
  public int max() {
    return casesMap.lastEntry().getKey();
  }

  @Override
  public int key(int index) {
    return casesMap.get(index).key();
  }

  @Override
  public void generateCase(int key, Label end) {
    generator.visit(context, casesMap.get(key).result(), stack);
    context.gotoLabel(end);
  }

  public int [] keys() {
    int [] keys = new int[casesMap.size()];

    int idx = 0;
    for (int key : casesMap.keySet()) {
      keys[idx++] = key;
    }
    return keys;
  }

  public void generateDefault() {
    if (defaultEval != null) {
      generator.visit(context, defaultEval, stack);
    } else {
      context.pushNullOfThreeValuedLogic();
      context.pushNullFlag(false);
    }
  }
}
