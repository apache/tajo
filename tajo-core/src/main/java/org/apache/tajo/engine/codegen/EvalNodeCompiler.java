/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.org.objectweb.asm.ClassWriter;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.offheap.OffHeapRowWriter;
import org.apache.tajo.tuple.offheap.RowWriter;
import org.apache.tajo.tuple.offheap.UnSafeTuple;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Stack;

import static org.apache.tajo.engine.codegen.TajoGeneratorAdapter.getDescription;

public class EvalNodeCompiler {
  private final TajoClassLoader classLoader;
  static int classSeq = 1;

  public EvalNodeCompiler(TajoClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public EvalNode compile(Schema schema, EvalNode eval) throws CompilationError {

    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    String className = EvalCodeGenerator.class.getPackage().getName() + ".CompiledEval" + classSeq++;
    String owner = TajoGeneratorAdapter.getInternalName(className);
    Variables variables = new Variables();
    emitMemberFields(variables, classWriter, eval);
    emitClassDefinition(owner, classWriter);
    emitConstructor(owner, classWriter, schema, variables);

    generateEvalFunc(owner, classWriter, schema, eval, variables);
    generateEvalFuncNative(owner, classWriter, schema, eval, variables);

    if (eval.getValueType().getType() == TajoDataTypes.Type.BOOLEAN) {
      generateIsMatchedFunc(owner, classWriter, schema, eval, variables);
    }

    classWriter.visitEnd();

    Class aClass = classLoader.defineClass(className, classWriter.toByteArray());

    Constructor constructor;
    EvalNode compiledEval;

    try {
      constructor = aClass.getConstructor();
      compiledEval = (EvalNode) constructor.newInstance();
    } catch (Throwable t) {
      throw new CompilationError(eval, t, classWriter.toByteArray());
    }
    return compiledEval;
  }

  public void emitClassDefinition(String className, ClassWriter classWriter) {
    classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, className, null,
        TajoGeneratorAdapter.getInternalName(EvalNode.class), null);
  }

  private void generateEvalFuncNative(String className, ClassWriter classWriter, Schema schema, EvalNode eval,
                                      Variables vars) {
    String evalDesc = TajoGeneratorAdapter.getMethodDescription(
        void.class,
        new Class[]{Schema.class, Tuple.class, RowWriter.class});

    EvalCodeGenContext evalContext = new EvalCodeGenContext(className,
        schema, classWriter, "eval", evalDesc, eval, vars);

    if (EvalCodeGenerator.isTextField(eval)) {
      copyTextOrBytes(evalContext, (FieldEval) eval);
    } else {
      EvalCodeGenerator.visit(evalContext, eval);
      evalContext.writeToTupleBuilder(eval.getValueType());
    }

    evalContext.methodvisitor.visitInsn(Opcodes.RETURN);
    evalContext.methodvisitor.visitMaxs(0, 0);
    evalContext.methodvisitor.visitEnd();
  }

  private void copyTextOrBytes(EvalCodeGenContext context, FieldEval field) {
    Column columnRef = field.getColumnRef();
    int fieldIdx;
    if (columnRef.hasQualifier()) {
      fieldIdx = context.schema.getColumnId(columnRef.getQualifiedName());
    } else {
      fieldIdx = context.schema.getColumnIdByName(columnRef.getSimpleName());
    }

    context.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
    context.emitIsNullOfTuple(fieldIdx); // It will push 1 if null, and it will push 0 if not null.

    Label ifNull = new Label();
    Label afterAll = new Label();
    // IFNE means if the first item in stack is not 0.
    context.methodvisitor.visitJumpInsn(Opcodes.IFNE, ifNull);

    context.aload(EvalCodeGenContext.BUILDER);
    context.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(OffHeapRowWriter.class));
    context.aload(EvalCodeGenContext.TUPLE);
    context.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(UnSafeTuple.class));
    context.push(fieldIdx);
    context.invokeVirtual(OffHeapRowWriter.class, "copyTextFrom", void.class, new Class [] {UnSafeTuple.class, int.class});
    context.gotoLabel(afterAll);

    context.methodvisitor.visitLabel(ifNull);
    context.aload(EvalCodeGenContext.BUILDER); // RowWriter
    context.invokeInterface(RowWriter.class, "skipField", void.class, new Class[]{});

    context.methodvisitor.visitLabel(afterAll);
  }

  private void generateEvalFunc(String className, ClassWriter classWriter, Schema schema, EvalNode eval,
                                      Variables vars) {
    String evalDesc = TajoGeneratorAdapter.getMethodDescription(
        Datum.class,
        new Class[]{Schema.class, Tuple.class});

    EvalCodeGenContext evalContext = new EvalCodeGenContext(className,
        schema, classWriter, "eval", evalDesc, eval, vars);
    EvalCodeGenerator.visit(evalContext, eval);
    evalContext.emitReturnAsDatum();
  }

  private void generateIsMatchedFunc(String className, ClassWriter classWriter, Schema schema, EvalNode eval, Variables vars) {
    String isMatchedDesc = TajoGeneratorAdapter.getMethodDescription(
        boolean.class,
        new Class[]{Schema.class, Tuple.class});

    EvalCodeGenContext isMatchedContext = new EvalCodeGenContext(className,
        schema, classWriter, "isMatched", isMatchedDesc, eval, vars);
    EvalCodeGenerator.visit(isMatchedContext, eval);
    isMatchedContext.emitReturnAsBool();
  }

  public void emitMemberFields(Variables variables, ClassWriter classWriter, EvalNode evalNode) {
    classWriter.visitField(Opcodes.ACC_PRIVATE, "schema",
        "L" + TajoGeneratorAdapter.getInternalName(Schema.class) + ";", null, null);

    VariablesBuilder builder = new VariablesBuilder(classWriter);
    builder.visit(variables, evalNode, new Stack<EvalNode>());
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

  public void emitConstructor(String className, ClassWriter classWriter, Schema schema, Variables variables) {
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
      initMethod.visitFieldInsn(Opcodes.PUTFIELD, className, "schema", getDescription(Schema.class));
    }

    for (Map.Entry<EvalNode, String> entry : variables.symbols.entrySet()) {
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
          initMethod.visitFieldInsn(Opcodes.PUTFIELD, className, entry.getValue(),
              "L" + TajoGeneratorAdapter.getInternalName(IntervalDatum.class) + ";");
        }

      } else if (entry.getKey().getType() == EvalType.IN) {
        InEval inEval = (InEval) entry.getKey();

        final String internalName = TajoGeneratorAdapter.getInternalName(InEval.class);
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
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, className, entry.getValue(), getDescription(InEval.class));

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
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, className, entry.getValue(), getDescription(clazz));

      } else if (entry.getKey().getType() == EvalType.FUNCTION) {
        GeneralFunctionEval function = (GeneralFunctionEval) entry.getKey();
        final String internalName = TajoGeneratorAdapter.getInternalName(function.getFuncDesc().getFuncClass());

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
        consAdapter.invokeVirtual(function.getFuncDesc().getFuncClass(), "init", void.class, new Class[] {FunctionEval.ParamType[].class});

        initMethod.visitVarInsn(Opcodes.ALOAD, 0);
        initMethod.visitVarInsn(Opcodes.ALOAD, FUNCTION);
        initMethod.visitFieldInsn(Opcodes.PUTFIELD, className, entry.getValue(),
            "L" + TajoGeneratorAdapter.getInternalName(function.getFuncDesc().getFuncClass()) + ";");

      }
    }

    initMethod.visitInsn(Opcodes.RETURN);
    initMethod.visitMaxs(1, 1);
    initMethod.visitEnd();
  }
}
