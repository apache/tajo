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

package org.apache.tajo.engine.codegen;

import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedInts;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.org.objectweb.asm.ClassWriter;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.storage.offheap.UnSafeTuple;
import org.apache.tajo.storage.offheap.UnSafeTupleTextComparator;
import org.apache.tajo.util.UnsafeComparer;

import java.lang.reflect.Constructor;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;
import static org.apache.tajo.engine.codegen.TajoGeneratorAdapter.getInternalName;

public class TupleComparerCompiler {
  private final static int LEFT_VALUE = 1;
  private final static int RIGHT_VALUE = 2;

  private static int classSeqId = 0;
  private final TajoClassLoader classLoader;

  public TupleComparerCompiler(TajoClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public TupleComparator compile(TupleComparatorImpl comp, boolean ensureUnSafeTuple) {
    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    String className = TupleComparator.class.getPackage().getName() + ".TupleComparator" + classSeqId++;

    emitClassDefinition(classWriter, getInternalName(className));
    emitConstructor(classWriter);
    emitCompare(classWriter, comp, ensureUnSafeTuple);

    classWriter.visitEnd();

    Class clazz = classLoader.defineClass(className, classWriter.toByteArray());
    Constructor constructor;
    TupleComparator compiled;

    try {
      constructor = clazz.getConstructor();
      compiled = (TupleComparator) constructor.newInstance();
    } catch (Throwable t) {
      throw new CompilationError(comp, t, classWriter.toByteArray());
    }
    return compiled;
  }

  private void emitClassDefinition(ClassWriter classWriter, String generatedClassName) {
    classWriter.visit(
        Opcodes.V1_6,
        Opcodes.ACC_PUBLIC,
        generatedClassName,
        null,
        getInternalName(TupleComparator.class),
        new String[]{}
    );
  }

  /**
   * Generation Constructor
   *
   * @param classWriter
   */
  private void emitConstructor(ClassWriter classWriter) {
    MethodVisitor constructorMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    constructorMethod.visitVarInsn(Opcodes.ALOAD, 0);
    constructorMethod.visitMethodInsn(Opcodes.INVOKESPECIAL,
        getInternalName(TupleComparator.class),
        "<init>",
        "()V");
    constructorMethod.visitInsn(Opcodes.RETURN);
    constructorMethod.visitMaxs(0, 0);
    constructorMethod.visitEnd();
  }

  /**
   * Generation Comparator::compare(Tuple t1, Tuple t2);
   *
   * This code generation makes use of subtraction for bool, short, integer
   *
   *
   * @param classWriter
   * @param compImpl
   */
  private void emitCompare(ClassWriter classWriter, TupleComparatorImpl compImpl, boolean ensureUnSafeTuple) {

    String methodName = "compare";
    String methodDesc = TajoGeneratorAdapter.getMethodDescription(int.class, new Class[]{Tuple.class, Tuple.class});
    MethodVisitor compMethod = classWriter.visitMethod(Opcodes.ACC_PUBLIC, methodName, methodDesc, null, null);
    compMethod.visitCode();
    compMethod.visitVarInsn(Opcodes.ALOAD, 0);

    TajoGeneratorAdapter adapter =
        new TajoGeneratorAdapter(Opcodes.ACC_PUBLIC, compMethod, methodName, methodDesc);

    final Label returnLabel = new Label();

    for (int idx = 0; idx < compImpl.getSortSpecs().length; idx++) {

      if (idx > 0) {
        // this check is omitted in the first field for comparison
        //
        // if cmpVal == 0 {
        //
        // } else {
        //   return cmpVal;
        // }
        adapter.dup();
        compMethod.visitJumpInsn(Opcodes.IFNE, returnLabel);
        compMethod.visitInsn(Opcodes.POP);
      }

      int nullFlag;

      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);

      adapter.push(compImpl.getSortKeyIds()[idx]);
      if (compImpl.getSortSpecs()[idx].isNullFirst()) {
        adapter.emitIsNotNullOfTuple();
      } else {
        adapter.emitIsNullOfTuple();
      }
      adapter.dup();
      nullFlag = adapter.istore();

      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      adapter.push(compImpl.getSortKeyIds()[idx]);
      if (compImpl.getSortSpecs()[idx].isNullFirst()) {
        adapter.emitIsNotNullOfTuple();

        adapter.dup();
        adapter.iload(nullFlag);
        compMethod.visitInsn(Opcodes.IAND);
        compMethod.visitVarInsn(Opcodes.ISTORE, nullFlag);
      } else {
        adapter.emitIsNullOfTuple();

        adapter.dup();
        adapter.iload(nullFlag);
        compMethod.visitInsn(Opcodes.IOR);
        compMethod.visitVarInsn(Opcodes.ISTORE, nullFlag);
      }

      compMethod.visitInsn(Opcodes.ISUB);

      adapter.dup();
      compMethod.visitJumpInsn(Opcodes.IFNE, returnLabel);
      adapter.pop();

      Label nextComp = new Label();
      Label pushComp = new Label();

      // nullFlag indicates if either value is null. If one of them is null, we should skip reading values.
      // For computation efficiency for null comparison, we use subtraction the results of isNull or isNotNull.
      // We reuse the result as follows:
      //
      // <For Null First>
      //
      // if (left.isNotNull && right.isNotNull) == FALSE, we can ensure one of them is NULL.
      //
      // <For Null Last>
      //
      // if (left.isNull || right.isNull) != FALSE, we can ensure one of them is NULL.

      adapter.iload(nullFlag);
      if (compImpl.getSortSpecs()[idx].isNullFirst()) {
        compMethod.visitJumpInsn(Opcodes.IFEQ, pushComp);
      } else {
        compMethod.visitJumpInsn(Opcodes.IFNE, pushComp);
      }

      SortSpec sortSpec = compImpl.getSortSpecs()[idx];
      DataType dataType = sortSpec.getSortKey().getDataType();

      if (dataType.getType() == Type.INET4) { // should be dealt as unsigned integers
        emitComparisonForUnsignedInts(adapter, compImpl, idx);
      } else if (TajoGeneratorAdapter.isJVMInternalInt(dataType)) {
        emitComparisonForJVMInteger(adapter, compImpl, idx);
      } else if (TajoGeneratorAdapter.getWordSize(dataType) == 2 || dataType.getType() == Type.FLOAT4) {
        emitComparisonForOtherPrimitives(adapter, compImpl, idx);
      } else if (dataType.getType() == Type.TEXT) {
        emitComparisonForText(adapter, compImpl, idx, ensureUnSafeTuple);
      } else {
        throw new UnsupportedException("Unknown sort type: " + dataType.getType().name());
      }
      compMethod.visitJumpInsn(Opcodes.GOTO, nextComp);

      compMethod.visitLabel(pushComp); // manually push compVal due to stack height balance
      adapter.push(0);

      compMethod.visitLabel(nextComp); // label for next value comparison
    }

    compMethod.visitLabel(returnLabel);
    compMethod.visitInsn(Opcodes.IRETURN);
    compMethod.visitMaxs(1, 0);
    compMethod.visitEnd();
  }

  private void emitGetParam(TajoGeneratorAdapter adapter, TupleComparatorImpl c, int idx, int paramIdx) {
    Preconditions.checkArgument(paramIdx == LEFT_VALUE || paramIdx == RIGHT_VALUE,
        "Param Index must be either 1 or 2.");

    // If sort order is a descending order, it switches left and right sides..
    boolean asc = c.getSortSpecs()[idx].isAscending();
    adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD,
        asc ? paramIdx : (paramIdx == LEFT_VALUE ? RIGHT_VALUE : LEFT_VALUE));
  }

  private void emitComparisonForJVMInteger(TajoGeneratorAdapter adapter, TupleComparatorImpl c, int idx) {
    emitGetParam(adapter, c, idx, LEFT_VALUE);
    adapter.emitGetValueOfTuple(c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    emitGetParam(adapter, c, idx, RIGHT_VALUE);
    adapter.emitGetValueOfTuple(c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    adapter.methodvisitor.visitInsn(Opcodes.ISUB);
  }

  private void emitComparisonForUnsignedInts(TajoGeneratorAdapter adapter, TupleComparatorImpl c, int idx) {
    emitGetParam(adapter, c, idx, LEFT_VALUE);
    adapter.emitGetValueOfTuple(c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    emitGetParam(adapter, c, idx, RIGHT_VALUE);
    adapter.emitGetValueOfTuple(c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    adapter.invokeStatic(UnsignedInts.class, "compare", int.class, new Class [] {int.class, int.class});
  }

  private void emitComparisonForOtherPrimitives(TajoGeneratorAdapter adapter, TupleComparatorImpl comp, int idx) {
    DataType dataType = comp.getSortSpecs()[idx].getSortKey().getDataType();
    emitGetParam(adapter, comp, idx, LEFT_VALUE);
    adapter.emitGetValueOfTuple(dataType, comp.getSortKeyIds()[idx]);
    int lhs = adapter.store(dataType);

    emitGetParam(adapter, comp, idx, RIGHT_VALUE);
    adapter.emitGetValueOfTuple(dataType, comp.getSortKeyIds()[idx]);
    int rhs = adapter.store(dataType);

    Label equalLabel = new Label();
    Label elseLabel = new Label();
    Label returnLabel = new Label();

    adapter.load(dataType, lhs);
    adapter.load(dataType, rhs);
    adapter.ifCmp(dataType, EvalType.LTH, equalLabel);
    adapter.push(-1);
    adapter.gotoLabel(returnLabel);

    adapter.methodvisitor.visitLabel(equalLabel);
    adapter.load(dataType, lhs);
    adapter.load(dataType, rhs);
    adapter.ifCmp(dataType, EvalType.EQUAL, elseLabel);
    adapter.push(0);
    adapter.gotoLabel(returnLabel);

    adapter.methodvisitor.visitLabel(elseLabel);
    adapter.push(1);
    adapter.methodvisitor.visitLabel(returnLabel);
  }

  private void emitComparisonForText(TajoGeneratorAdapter adapter, TupleComparatorImpl c, int idx,
                                     boolean ensureUnSafeTuple) {
    if (ensureUnSafeTuple) {
      emitGetParam(adapter, c, idx, LEFT_VALUE);
      adapter.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, getInternalName(UnSafeTuple.class));
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeVirtual(UnSafeTuple.class, "getFieldAddr", long.class, new Class[]{int.class});

      emitGetParam(adapter, c, idx, RIGHT_VALUE);
      adapter.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, getInternalName(UnSafeTuple.class));
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeVirtual(UnSafeTuple.class, "getFieldAddr", long.class, new Class[]{int.class});

      adapter.invokeStatic(UnSafeTupleTextComparator.class, "compare", int.class, new Class[]{long.class, long.class});
    } else {
      emitGetParam(adapter, c, idx, LEFT_VALUE);
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeInterface(Tuple.class, "getBytes", byte [].class, new Class [] {int.class});

      emitGetParam(adapter, c, idx, RIGHT_VALUE);
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeInterface(Tuple.class, "getBytes", byte [].class, new Class [] {int.class});

      adapter.invokeStatic(UnsafeComparer.class, "compareStatic", int.class, new Class[]{byte[].class, byte[].class});
    }
  }
}
