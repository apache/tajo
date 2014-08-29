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

import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.eval.EvalType;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.org.objectweb.asm.ClassWriter;
import org.apache.tajo.org.objectweb.asm.Label;
import org.apache.tajo.org.objectweb.asm.MethodVisitor;
import org.apache.tajo.org.objectweb.asm.Opcodes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.storage.directmem.UnSafeTuple;
import org.apache.tajo.storage.directmem.UnSafeTupleTextComparator;
import org.apache.tajo.util.UnsafeComparator;

import java.lang.reflect.Constructor;

import static org.apache.tajo.common.TajoDataTypes.DataType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class TupleComparatorCompiler {
  private static int classSeqId = 0;
  private final TajoClassLoader classLoader;

  public TupleComparatorCompiler(TajoClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  public TupleComparator compile(TupleComparatorImpl comp, boolean ensureUnSafeTuple) {
    ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);

    String className = TupleComparator.class.getPackage().getName() + ".TupleComparator" + classSeqId++;

    emitClassDefinition(classWriter, TajoGeneratorAdapter.getInternalName(className));
    emitConstructor(classWriter);
    emitCompare(classWriter, comp, ensureUnSafeTuple);

    classWriter.visitEnd();

    Class clazz = classLoader.defineClass(className, classWriter.toByteArray());
    Constructor constructor;
    TupleComparator compiled;

    System.out.println(CodeGenUtils.disassemble(classWriter.toByteArray()));

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
        Opcodes.V1_5,
        Opcodes.ACC_PUBLIC,
        generatedClassName,
        null,
        TajoGeneratorAdapter.getInternalName(TupleComparator.class),
        new String [] {}
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
        TajoGeneratorAdapter.getInternalName(TupleComparator.class),
        "<init>",
        "()V");
    constructorMethod.visitInsn(Opcodes.RETURN);
    constructorMethod.visitMaxs(0, 0);
    constructorMethod.visitEnd();
  }

  /**
   * Generation Comparator::compare(Tuple t1, Tuple t2);
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

    TajoGeneratorAdapter generatorAdapter =
        new TajoGeneratorAdapter(Opcodes.ACC_PUBLIC, compMethod, methodName, methodDesc);

    final Label returnLabel = new Label();

    for (int idx = 0; idx < compImpl.getSortSpecs().length; idx++) {

      if (idx > 0) {
        // if cmpVal == 0 {
        //
        // } else {
        //   return cmpVal;
        // }
        generatorAdapter.dup();
        generatorAdapter.push(0);
        compMethod.visitJumpInsn(Opcodes.IF_ICMPNE, returnLabel);
        compMethod.visitInsn(Opcodes.POP);
      }

//      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);
//      adapter.invokeInterface(Tuple.class, "isNotNull", boolean.class, new Class [] {int.class});
//
//      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
//      adapter.invokeInterface(Tuple.class, "isNotNull", boolean.class, new Class [] {int.class});
//
//      compMethod.visitInsn(Opcodes.ISUB);
//
//      adapter.dup();
//      adapter.push(0);
//      compMethod.visitJumpInsn(Opcodes.IF_ICMPNE, returnLabel);
      //adapter.pop();

      SortSpec sortSpec = compImpl.getSortSpecs()[idx];
      DataType dataType = sortSpec.getSortKey().getDataType();

      if (TajoGeneratorAdapter.isJVMInternalInt(dataType)) {
        emitComparisonForJVMInteger(generatorAdapter, compImpl, idx);
      } else if (TajoGeneratorAdapter.getWordSize(dataType) == 2 || dataType.getType() == Type.FLOAT4) {
        emitComparisonForOtherPrimitives(generatorAdapter, compImpl, idx);
      } else if (dataType.getType() == Type.TEXT) {
        emitComparisonForText(generatorAdapter, compImpl, idx, ensureUnSafeTuple);
      } else {
        throw new UnsupportedException("Unknown sort type: " + dataType.getType().name());
      }
    }

    // column 1

    // some routine which return integer;
    //
    // bool:
    //
    // true - true = 0,
    // true (1) - false (2)
    // false - true 1
    //
    // int1, int2, int4, date, inet4 -> left value - right value

    // Other than JVM integer types (long, float, double, timestamp, time)
    //
    // if x.c1 < y.c1
    //   compVal = -1;
    // else if (x.c2 == y.c2) {
    //   return 0;
    // else
    //   return 1;

    // text (Improve UnsignedBytes to directly access bytes)

    // proto - non comparable
    compMethod.visitLabel(returnLabel);
    compMethod.visitInsn(Opcodes.IRETURN);
    compMethod.visitMaxs(1, 0);
    compMethod.visitEnd();
  }

  private void emitComparisonForJVMInteger(TajoGeneratorAdapter adapter, TupleComparatorImpl c, int idx) {

    adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);
    emitGetJVMIntValue(adapter, c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
    emitGetJVMIntValue(adapter, c.getSortSpecs()[idx].getSortKey().getDataType(), c.getSortKeyIds()[idx]);

    adapter.methodvisitor.visitInsn(Opcodes.ISUB);
  }

  private void emitGetJVMIntValue(TajoGeneratorAdapter adapter, DataType dataType, int fieldIndex) {
    adapter.push(fieldIndex);

    Type type = dataType.getType();
    switch (type) {
    case INT1:
    case INT2:
      adapter.invokeInterface(Tuple.class, "getInt2", short.class, new Class[]{int.class});
      break;
    case INT4:
    case DATE:
    case INET4:
      adapter.invokeInterface(Tuple.class, "getInt4", int.class, new Class[]{int.class});
      break;
    case INT8:
    case TIMESTAMP:
    case TIME:
      adapter.invokeInterface(Tuple.class, "getInt8", long.class, new Class[]{int.class});
      break;
    case FLOAT4:
      adapter.invokeInterface(Tuple.class, "getFloat4", float.class, new Class[]{int.class});
      break;
    case FLOAT8:
      adapter.invokeInterface(Tuple.class, "getFloat8", double.class, new Class[]{int.class});
      break;
    case TEXT:
      adapter.invokeInterface(Tuple.class, "getText", String.class, new Class[]{int.class});
      break;
    case INTERVAL:
      adapter.invokeInterface(Tuple.class, "getInterval", IntervalDatum.class, new Class[]{int.class});
      break;
    default:
      throw new UnsupportedException("Unknown data type: " + type.name());
    }
  }

  private void emitComparisonForOtherPrimitives(TajoGeneratorAdapter adapter, TupleComparatorImpl comp, int idx) {
    DataType dataType = comp.getSortSpecs()[idx].getSortKey().getDataType();

    adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);
    emitGetJVMIntValue(adapter, dataType, comp.getSortKeyIds()[idx]);
    int lhs = adapter.store(dataType);

    adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
    emitGetJVMIntValue(adapter, dataType, comp.getSortKeyIds()[idx]);
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
      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);
      adapter.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(UnSafeTuple.class));
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeVirtual(UnSafeTuple.class, "getFieldAddr", long.class, new Class[]{int.class});

      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      adapter.methodvisitor.visitTypeInsn(Opcodes.CHECKCAST, TajoGeneratorAdapter.getInternalName(UnSafeTuple.class));
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeVirtual(UnSafeTuple.class, "getFieldAddr", long.class, new Class[]{int.class});

      adapter.invokeStatic(UnSafeTupleTextComparator.class, "compare", int.class, new Class[]{long.class, long.class});
    } else {
      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 1);
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeInterface(Tuple.class, "getBytes", byte [].class, new Class [] {int.class});

      adapter.methodvisitor.visitVarInsn(Opcodes.ALOAD, 2);
      adapter.push(c.getSortKeyIds()[idx]);
      adapter.invokeInterface(Tuple.class, "getBytes", byte [].class, new Class [] {int.class});

      adapter.invokeStatic(UnsafeComparator.class, "compareStatic", int.class, new Class[]{byte[].class, byte[].class});
    }
  }
}
