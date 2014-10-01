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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.org.objectweb.asm.ClassWriter;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.org.objectweb.asm.commons.GeneratorAdapter;

public class EvalCodeGenContext extends TajoGeneratorAdapter {
  final String owner;
  final Schema schema;
  final ClassWriter classWriter;
  final EvalNode evalNode;
  final Variables variables;

  public EvalCodeGenContext(String className, Schema schema, ClassWriter classWriter, String methodName,
                            String methodDesc, EvalNode evalNode, Variables variables) {
    this.owner = className;
    this.classWriter = classWriter;
    this.schema = schema;
    this.evalNode = evalNode;
    this.variables = variables;

    MethodVisitor evalMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, methodName, methodDesc, null, null);
    evalMethod.visitCode();
    this.methodvisitor = evalMethod;
    generatorAdapter = new GeneratorAdapter(this.methodvisitor, access, methodDesc, methodDesc);
  }

  public void emitReturnAsDatum() {
    convertToDatum(evalNode.getValueType(), true);
    methodvisitor.visitInsn(Opcodes.ARETURN);
    methodvisitor.visitMaxs(0, 0);
    methodvisitor.visitEnd();
  }

  public void emitReturnAsBool() {
    returnAsBool();
    methodvisitor.visitMaxs(0, 0);
    methodvisitor.visitEnd();
  }
}
