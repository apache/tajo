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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.org.objectweb.asm.ClassWriter;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.org.objectweb.asm.commons.GeneratorAdapter;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.storage.Tuple;

import java.util.Map;
import java.util.Stack;

public class EvalCodeGenContext extends TajoGeneratorAdapter {
  final String owner;
  final Schema schema;
  final ClassWriter classWriter;
  final EvalNode evalNode;
  final Map<EvalNode, String> symbols;
  int seqId = 0;

  public EvalCodeGenContext(String className, Schema schema, ClassWriter classWriter, EvalNode evalNode) {
    this.owner = className;
    this.classWriter = classWriter;
    this.schema = schema;
    this.evalNode = evalNode;
    this.symbols = Maps.newHashMap();

    emitClassDefinition();
    emitMemberFields();
    classWriter.visitEnd();
    emitConstructor();

    String methodName = "eval";
    String methodDesc = TajoGeneratorAdapter.getMethodDescription(Datum.class, new Class[]{Tuple.class});
    MethodVisitor evalMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, methodName, methodDesc, null, null);
    evalMethod.visitCode();
    this.methodvisitor = evalMethod;
    generatorAdapter = new GeneratorAdapter(this.methodvisitor, access, methodDesc, methodDesc);
  }

  public void emitClassDefinition() {
    classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, this.owner, null,
        TajoGeneratorAdapter.getInternalName(EvalNode.class), null);
  }

  public void emitMemberFields() {
    classWriter.visitField(Opcodes.ACC_PRIVATE, "schema",
        "L" + TajoGeneratorAdapter.getInternalName(Schema.class) + ";", null, null);

    VariablesPreBuilder builder = new VariablesPreBuilder();
    builder.visit(this, evalNode, new Stack<EvalNode>());
  }

  public static void emitCreateSchema(TajoGeneratorAdapter adapter, MethodVisitor mv, Schema schema) {
    mv.visitLdcInsn(schema.toJson());
    adapter.invokeStatic(EvalCodeGenerator.class, "createSchema", Schema.class, new Class[] {String.class});
  }

  public static void emitCreateEval(TajoGeneratorAdapter adapter, MethodVisitor mv, EvalNode evalNode) {
    mv.visitLdcInsn(evalNode.toJson());
    adapter.invokeStatic(EvalCodeGenerator.class, "createEval", EvalNode.class, new Class[] {String.class});
  }

  public static void emitConstEval(TajoGeneratorAdapter adapter, MethodVisitor mv, ConstEval evalNode) {
    mv.visitLdcInsn(evalNode.toJson());
    adapter.invokeStatic(EvalCodeGenerator.class, "createConstEval", ConstEval.class, new Class[] {String.class});
  }

  public static void emitRowConstantEval(TajoGeneratorAdapter adapter, MethodVisitor mv, RowConstantEval evalNode) {
    mv.visitLdcInsn(evalNode.toJson());
    adapter.invokeStatic(EvalCodeGenerator.class, "createRowConstantEval", RowConstantEval.class,
        new Class[] {String.class});
  }

  public void emitConstructor() {
    // constructor method
    MethodVisitor initMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    initMethod.visitCode();
    initMethod.visitVarInsn(Opcodes.ALOAD, 0);
    initMethod.visitMethodInsn(Opcodes.INVOKESPECIAL, TajoGeneratorAdapter.getInternalName(EvalNode.class), "<init>",
        "()V");

    TajoGeneratorAdapter consAdapter = new TajoGeneratorAdapter(Opcodes.ACC_PUBLIC, initMethod, "<init>", "()V");

    // == this.schema = schema;
    if (schema != null) {
      consAdapter.aload(0);
      emitCreateSchema(consAdapter, initMethod, schema);
      initMethod.visitFieldInsn(Opcodes.PUTFIELD, this.owner, "schema", getDescription(Schema.class));
    }

    for (Map.Entry<EvalNode, String> entry : symbols.entrySet()) {
      if (entry.getKey().getType() == EvalType.CONST) {
        ConstEval constEval = (ConstEval) entry.getKey();

        if (constEval.getValueType().getType() == TajoDataTypes.Type.INTERVAL) {
          IntervalDatum datum = (IntervalDatum) constEval.getValue();

          final String internalName = TajoGeneratorAdapter.getInternalName(IntervalDatum.class);

          initMethod.visitTypeInsn(Opcodes.NEW, internalName);
          consAdapter.dup();
          initMethod.visitLdcInsn(datum.getMonths());
          initMethod.visitLdcInsn(datum.getMilliSeconds());
          initMethod.visitMethodInsn(Opcodes.INVOKESPECIAL, internalName, "<init>", "(IJ)V");
          int INTERVAL_DATUM = consAdapter.astore();

          consAdapter.aload(0);
          consAdapter.aload(INTERVAL_DATUM);
          initMethod.visitFieldInsn(Opcodes.PUTFIELD, this.owner, entry.getValue(),
              "L" + TajoGeneratorAdapter.getInternalName(IntervalDatum.class) + ";");
        }

      } else if (entry.getKey().getType() == EvalType.IN) {
        InEval inEval = (InEval) entry.getKey();

        final String internalName = getInternalName(InEval.class);
        initMethod.visitTypeInsn(Opcodes.NEW, internalName);
        consAdapter.dup();
        emitCreateEval(consAdapter, initMethod, inEval.getLeftExpr());
        emitRowConstantEval(consAdapter, initMethod, (RowConstantEval) inEval.getRightExpr());
        consAdapter.push(inEval.isNot());
        consAdapter.invokeSpecial(InEval.class, "<init>", void.class,
            new Class [] {EvalNode.class, RowConstantEval.class, boolean.class});
        int IN_PREDICATE_EVAL = consAdapter.astore();

        consAdapter.aload(0);
        consAdapter.aload(IN_PREDICATE_EVAL);
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, this.owner, entry.getValue(), getDescription(InEval.class));

      } else if (EvalType.isStringPatternMatchOperator(entry.getKey().getType())) {
        PatternMatchPredicateEval patternPredicate = (PatternMatchPredicateEval) entry.getKey();

        Class clazz = EvalCodeGenerator.getStringPatternEvalClass(entry.getKey().getType());
        final String internalName = TajoGeneratorAdapter.getInternalName(clazz);

        initMethod.visitTypeInsn(Opcodes.NEW, internalName);
        consAdapter.dup();
        consAdapter.push(patternPredicate.isNot());
        emitCreateEval(consAdapter, initMethod, patternPredicate.getLeftExpr());
        emitConstEval(consAdapter, initMethod, (ConstEval) patternPredicate.getRightExpr());
        consAdapter.push(patternPredicate.isCaseInsensitive());
        consAdapter.invokeSpecial(clazz, "<init>", void.class,
            new Class [] {boolean.class, EvalNode.class, ConstEval.class, boolean.class});

        int PatternEval = consAdapter.astore();

        consAdapter.aload(0);
        consAdapter.aload(PatternEval);
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, this.owner, entry.getValue(), getDescription(clazz));

      } else if (entry.getKey().getType() == EvalType.FUNCTION) {
        GeneralFunctionEval function = (GeneralFunctionEval) entry.getKey();
        final String internalName = TajoGeneratorAdapter.getInternalName(function.getFuncDesc().getLegacyFuncClass());

        // new and initialization of function
        initMethod.visitTypeInsn(Opcodes.NEW, internalName);
        consAdapter.dup();
        initMethod.visitMethodInsn(Opcodes.INVOKESPECIAL, internalName, "<init>", "()V");
        int FUNCTION = consAdapter.astore();

        // commParam
        int paramNum = function.getArgs().length;
        initMethod.visitLdcInsn(paramNum);
        consAdapter.newArray(FunctionEval.ParamType.class);
        final int PARAM_TYPE_ARRAY = consAdapter.astore();
        FunctionEval.ParamType[] paramTypes = EvalCodeGenerator.getParamTypes(function.getArgs());
        for (int paramIdx = 0; paramIdx < paramTypes.length; paramIdx++) {
          consAdapter.aload(PARAM_TYPE_ARRAY);
          consAdapter.methodvisitor.visitLdcInsn(paramIdx);
          consAdapter.methodvisitor.visitFieldInsn(Opcodes.GETSTATIC, TajoGeneratorAdapter.getInternalName(FunctionEval.ParamType.class),
              paramTypes[paramIdx].name(), TajoGeneratorAdapter.getDescription(FunctionEval.ParamType.class));
          consAdapter.methodvisitor.visitInsn(Opcodes.AASTORE);
        }

        initMethod.visitVarInsn(Opcodes.ALOAD, FUNCTION);
        consAdapter.aload(PARAM_TYPE_ARRAY);
        consAdapter.invokeVirtual(function.getFuncDesc().getLegacyFuncClass(), "init", void.class, new Class[] {FunctionEval.ParamType[].class});

        initMethod.visitVarInsn(Opcodes.ALOAD, 0);
        initMethod.visitVarInsn(Opcodes.ALOAD, FUNCTION);
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, this.owner, entry.getValue(),
            "L" + TajoGeneratorAdapter.getInternalName(function.getFuncDesc().getLegacyFuncClass()) + ";");

      }
    }

    initMethod.visitInsn(Opcodes.RETURN);
    initMethod.visitMaxs(1, 1);
    initMethod.visitEnd();
  }

  public void emitReturn() {
    convertToDatum(evalNode.getValueType(), true);
    methodvisitor.visitInsn(Opcodes.ARETURN);
    methodvisitor.visitMaxs(0, 0);
    methodvisitor.visitEnd();
    classWriter.visitEnd();
  }
}
