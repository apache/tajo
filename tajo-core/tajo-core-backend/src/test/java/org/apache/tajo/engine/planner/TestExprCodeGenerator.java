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

package org.apache.tajo.engine.planner;


import org.apache.tajo.LocalTajoTestingUtility;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.Selection;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.engine.eval.BasicEvalNodeVisitor;
import org.apache.tajo.engine.eval.BinaryEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.TestEvalTreeUtil;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.CommonTestingUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Stack;

import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;

public class TestExprCodeGenerator {

  static TajoTestingCluster util;
  static CatalogService catalog = null;
  static EvalNode expr1;
  static EvalNode expr2;
  static EvalNode expr3;
  static SQLAnalyzer analyzer;
  static LogicalPlanner planner;
  static Session session = LocalTajoTestingUtility.createDummySession();

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.createFunction(funcDesc);
    }
    catalog.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    catalog.createDatabase(TajoConstants.DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);

    Schema schema = new Schema();
    schema.addColumn("name", TajoDataTypes.Type.TEXT);
    schema.addColumn("score", TajoDataTypes.Type.INT4);
    schema.addColumn("age", TajoDataTypes.Type.INT4);

    TableMeta meta = CatalogUtil.newTableMeta(CatalogProtos.StoreType.CSV);
    TableDesc desc = new TableDesc(
        CatalogUtil.buildFQName(TajoConstants.DEFAULT_DATABASE_NAME, "people"), schema, meta,
        CommonTestingUtil.getTestDir());
    catalog.createTable(desc);

    FunctionDesc funcMeta = new FunctionDesc("test_sum", TestEvalTreeUtil.TestSum.class,
        CatalogProtos.FunctionType.GENERAL,
        CatalogUtil.newSimpleDataType(TajoDataTypes.Type.INT4),
        CatalogUtil.newSimpleDataTypeArray(TajoDataTypes.Type.INT4, TajoDataTypes.Type.INT4));
    catalog.createFunction(funcMeta);

    analyzer = new SQLAnalyzer();
    planner = new LogicalPlanner(catalog);

    String[] QUERIES = {
        "select name, score, age from people where score > 30", // 0
        "select name, score, age from people where score * age", // 1
        "select name, score, age from people where test_sum(score * age, 50)", // 2
    };

    expr1 = getRootSelection(QUERIES[0]);
    expr2 = getRootSelection(QUERIES[1]);
    expr3 = getRootSelection(QUERIES[2]);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  public static EvalNode getRootSelection(String query) throws PlanningException {
    Expr block = analyzer.parse(query);
    LogicalPlan plan = null;
    try {
      plan = planner.createPlan(session, block);
    } catch (PlanningException e) {
      e.printStackTrace();
    }

    Selection selection = plan.getRootBlock().getSingletonExpr(OpType.Filter);
    return planner.getExprAnnotator().createEvalNode(plan, plan.getRootBlock(), selection.getQual());
  }

  public static class CodeGenContext {
    private ClassWriter classWriter;
    public CodeGenContext() {
      classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
      classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", null);
      classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
          null, null).visitEnd();

      MethodVisitor methodVisitor = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
      methodVisitor.visitCode();
      methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
      methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", "<init>", "()V");
      methodVisitor.visitInsn(Opcodes.RETURN);
      methodVisitor.visitMaxs(1, 1);
      methodVisitor.visitEnd();
    }
  }

  public class EvalGen {
    public Datum eval(int x, int y) {
      return new Int4Datum(x + y);
    }
  }

  public class ExprCodeGenerator extends BasicEvalNodeVisitor<CodeGenContext, Object> {

    public EvalNode generate(EvalNode expr) throws NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, InstantiationException {
      return null;
    }

    public Object visitPlus(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
      return null;
    }
  }

  @Test
  public void testGenerateCode() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    MyClassLoader myClassLoader = new MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test2", getBytecodeForClass());
    System.out.println(aClass.getSimpleName());
    Constructor constructor = aClass.getConstructor();
    Example r = (Example) constructor.newInstance();
    r.run("test");
  }

  public static class Example {
    public void run(String msg) {
      System.out.println(msg);
    }
  }

  @Test
  public void testGenerateObjectReturn() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    MyClassLoader myClassLoader = new MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test3", getBytecodeForObjectReturn());
//    System.out.println(aClass.getSimpleName());
    Constructor constructor = aClass.getConstructor();
    NewMockUp r = (NewMockUp) constructor.newInstance();
    System.out.println(r.eval(1, 5));
  }



  public static class NewMockUp {
    public Datum eval(int x, int y) {
      return null;
    }
  }

  public static byte[] getBytecodeForObjectReturn() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$NewMockUp", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();

    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "eval", "(II)Lorg/apache/tajo/datum/Datum;", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);

    methodVisitor.visitTypeInsn(Opcodes.NEW, "org/apache/tajo/datum/Int4Datum");
    methodVisitor.visitInsn(Opcodes.DUP);

    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 2);
    methodVisitor.visitInsn(Opcodes.IADD);

    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/datum/Int4Datum", "<init>", "(I)V");
    methodVisitor.visitTypeInsn(Opcodes.CHECKCAST, "org/apache/tajo/datum/Datum");
    methodVisitor.visitInsn(Opcodes.ARETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  @Test
  public void testGenerateCodePlus() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException {
    MyClassLoader myClassLoader = new MyClassLoader();
    Class aClass = myClassLoader.defineClass("org.Test2", getBytecodeForPlus());
//    System.out.println(aClass.getSimpleName());
    Constructor constructor = aClass.getConstructor();
    PlusExpr r = (PlusExpr) constructor.newInstance();
    System.out.println(r.eval(1, 3));
  }

  public static class PlusExpr {
    public int eval(int x, int y) {
      return x + y;
    }
  }

  public static byte[] getBytecodeForPlus() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test2", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$PlusExpr", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    System.out.println(Opcodes.ACC_PUBLIC);
    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$PlusExpr", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();

    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "eval", "(II)I", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 1);
    methodVisitor.visitVarInsn(Opcodes.ILOAD, 2);
    methodVisitor.visitInsn(Opcodes.IADD);
    methodVisitor.visitInsn(Opcodes.IRETURN);
    methodVisitor.visitMaxs(0, 0);
    methodVisitor.visitEnd();
    cw.visitEnd();
    return cw.toByteArray();
  }

  public static byte[] getBytecodeForClass() {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
    cw.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test2", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", null);
    cw.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
        null, null).visitEnd();

    System.out.println(Opcodes.ACC_PUBLIC);
    MethodVisitor methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", "<init>", "()V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(1, 1);
    methodVisitor.visitEnd();


    methodVisitor = cw.visitMethod(Opcodes.ACC_PUBLIC, "run", "(Ljava/lang/String;)V", null, null);
    methodVisitor.visitCode();
    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "currentTimeMillis", "()J");
    methodVisitor.visitVarInsn(Opcodes.LSTORE, 2);
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
    methodVisitor.visitVarInsn(Opcodes.ALOAD, 1);
    methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$Example", "run", "(Ljava/lang/String;)V");
    methodVisitor.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/System", "out", "Ljava/io/PrintStream;");
    methodVisitor.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "currentTimeMillis", "()J");
    methodVisitor.visitVarInsn(Opcodes.LLOAD, 2);
    methodVisitor.visitInsn(Opcodes.LSUB);
    methodVisitor.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/io/PrintStream", "println", "(J)V");
    methodVisitor.visitInsn(Opcodes.RETURN);
    methodVisitor.visitMaxs(5, 4);
    methodVisitor.visitEnd();

    cw.visitEnd();
    return cw.toByteArray();
  }

  class MyClassLoader extends ClassLoader {
    public Class defineClass(String name, byte[] b) {
      return defineClass(name, b, 0, b.length);
    }
  }
}
