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
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.cli.InvalidStatementException;
import org.apache.tajo.cli.ParsedResult;
import org.apache.tajo.cli.SimpleParser;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.parser.SQLAnalyzer;
import org.apache.tajo.master.TajoMaster;
import org.apache.tajo.master.session.Session;
import org.apache.tajo.util.CodeGenUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Stack;

import static org.apache.tajo.TajoConstants.DEFAULT_DATABASE_NAME;
import static org.apache.tajo.TajoConstants.DEFAULT_TABLESPACE_NAME;
import static org.junit.Assert.assertFalse;

public class TestExprCodeGenerator {

  private static TajoTestingCluster util;
  private static CatalogService cat;
  private static SQLAnalyzer analyzer;
  private static PreLogicalPlanVerifier preLogicalPlanVerifier;
  private static LogicalPlanner planner;
  private static LogicalOptimizer optimizer;
  private static LogicalPlanVerifier annotatedPlanVerifier;

  @BeforeClass
  public static void setUp() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    cat = util.getMiniCatalogCluster().getCatalog();
    cat.createTablespace(DEFAULT_TABLESPACE_NAME, "hdfs://localhost:1234/warehouse");
    cat.createDatabase(DEFAULT_DATABASE_NAME, DEFAULT_TABLESPACE_NAME);
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      cat.createFunction(funcDesc);
    }

    analyzer = new SQLAnalyzer();
    preLogicalPlanVerifier = new PreLogicalPlanVerifier(cat);
    planner = new LogicalPlanner(cat);
    optimizer = new LogicalOptimizer(util.getConfiguration());
    annotatedPlanVerifier = new LogicalPlanVerifier(util.getConfiguration(), cat);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownCatalogCluster();
  }

  /**
   * verify query syntax and get raw targets.
   *
   * @param query a query for execution
   * @param condition this parameter means whether it is for success case or is not for failure case.
   * @return
   * @throws PlanningException
   */
  private static Target[] getRawTargets(String query, boolean condition) throws PlanningException,
      InvalidStatementException {

    Session session = LocalTajoTestingUtility.createDummySession();
    List<ParsedResult> parsedResults = SimpleParser.parseScript(query);
    if (parsedResults.size() > 1) {
      throw new RuntimeException("this query includes two or more statements.");
    }
    Expr expr = analyzer.parse(parsedResults.get(0).getStatement());
    VerificationState state = new VerificationState();
    preLogicalPlanVerifier.verify(session, state, expr);
    if (state.getErrorMessages().size() > 0) {
      if (!condition && state.getErrorMessages().size() > 0) {
        throw new PlanningException(state.getErrorMessages().get(0));
      }
      assertFalse(state.getErrorMessages().get(0), true);
    }
    LogicalPlan plan = planner.createPlan(session, expr, true);
    optimizer.optimize(plan);
    annotatedPlanVerifier.verify(session, state, plan);

    if (state.getErrorMessages().size() > 0) {
      assertFalse(state.getErrorMessages().get(0), true);
    }

    Target [] targets = plan.getRootBlock().getRawTargets();
    if (targets == null) {
      throw new PlanningException("Wrong query statement or query plan: " + parsedResults.get(0).getStatement());
    }
    return targets;
  }

  public static class CodeGenContext {
    private ClassWriter classWriter;
    private MethodVisitor evalMethod;

    public CodeGenContext() {
      classWriter = new ClassWriter(ClassWriter.COMPUTE_MAXS);
      classWriter.visit(Opcodes.V1_5, Opcodes.ACC_PUBLIC, "org/Test3", null, "org/apache/tajo/engine/planner/TestExprCodeGenerator$EvalGen", null);
      classWriter.visitField(Opcodes.ACC_PRIVATE, "name", "Ljava/lang/String;",
          null, null).visitEnd();

      // constructor method
      MethodVisitor methodVisitor = classWriter.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
      methodVisitor.visitCode();
      methodVisitor.visitVarInsn(Opcodes.ALOAD, 0);
      methodVisitor.visitMethodInsn(Opcodes.INVOKESPECIAL, "org/apache/tajo/engine/planner/TestExprCodeGenerator$EvalGen", "<init>", "()V");
      methodVisitor.visitInsn(Opcodes.RETURN);
      methodVisitor.visitMaxs(1, 1);
      methodVisitor.visitEnd();
    }
  }

  public static class EvalGen {
    public Datum eval(int x, int y) {
      return null;
    }
  }

  public static String getClassName(Class clazz) {
    return clazz.getName().replace('.', '/');
  }

  public static class ExprCodeGenerator extends BasicEvalNodeVisitor<CodeGenContext, Object> {

    public EvalGen generate(EvalNode expr) throws NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, InstantiationException, PlanningException {
      CodeGenContext context = new CodeGenContext();

      // evalMethod
      context.evalMethod = context.classWriter.visitMethod(Opcodes.ACC_PUBLIC, "eval",
          "(II)Lorg/apache/tajo/datum/Datum;", null, null);
      context.evalMethod.visitCode();
      context.evalMethod.visitVarInsn(Opcodes.ALOAD, 0);

      Class returnTypeClass;
      String desc;
      switch (expr.getValueType().getType()) {
      case INT2:
        returnTypeClass = Int2Datum.class;
        desc = "(S)V";
        break;
      case INT4:
        returnTypeClass = Int4Datum.class;
        desc = "(I)V";
        break;
      case INT8:
        returnTypeClass = Int8Datum.class;
        desc = "(L)V";
        break;
      case FLOAT4:
        returnTypeClass = Float4Datum.class;
        desc = "(F)V";
        break;
      case FLOAT8:
        returnTypeClass = Float8Datum.class;
        desc = "(D)V";
        break;
      default:
        throw new PlanningException("Unsupported type: " + expr.getValueType().getType());
      }

      context.evalMethod.visitTypeInsn(Opcodes.NEW, getClassName(returnTypeClass));
      context.evalMethod.visitInsn(Opcodes.DUP);

      visitChild(context, expr, new Stack<EvalNode>());

      context.evalMethod.visitMethodInsn(Opcodes.INVOKESPECIAL, getClassName(returnTypeClass), "<init>", desc);
      context.evalMethod.visitTypeInsn(Opcodes.CHECKCAST, getClassName(Datum.class));
      context.evalMethod.visitInsn(Opcodes.ARETURN);
      context.evalMethod.visitMaxs(0, 0);
      context.evalMethod.visitEnd();
      context.classWriter.visitEnd();

      MyClassLoader myClassLoader = new MyClassLoader();
      Class aClass = myClassLoader.defineClass("org.Test3", context.classWriter.toByteArray());
      Constructor constructor = aClass.getConstructor();
      EvalGen r = (EvalGen) constructor.newInstance();
      return r;
    }

    public Object visitPlus(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
      super.visitPlus(context, evalNode, stack);

      int opcode;
      switch (evalNode.getValueType().getType()) {
      case INT4:
        opcode = Opcodes.IADD;
        break;
      case INT8:
        opcode = Opcodes.LADD;
        break;
      case FLOAT4:
        opcode = Opcodes.FADD;
        break;
      case FLOAT8:
        opcode = Opcodes.DADD;
        break;
      default:
        throw new RuntimeException("Plus does not support:" + evalNode.getValueType().getType());
      }

      context.evalMethod.visitInsn(opcode);

      return null;
    }

    public Object visitMinus(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
      super.visitMinus(context, evalNode, stack);

      int opcode;
      switch (evalNode.getValueType().getType()) {
      case INT4:
        opcode = Opcodes.ISUB;
        break;
      case INT8:
        opcode = Opcodes.LSUB;
        break;
      case FLOAT4:
        opcode = Opcodes.FSUB;
        break;
      case FLOAT8:
        opcode = Opcodes.DSUB;
        break;
      default:
        throw new RuntimeException("Plus does not support:" + evalNode.getValueType().getType());
      }

      context.evalMethod.visitInsn(opcode);

      return null;
    }

    @Override
    public Object visitMultiply(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
      super.visitMultiply(context, evalNode, stack);

      int opcode;
      switch (evalNode.getValueType().getType()) {
      case INT4:
        opcode = Opcodes.IMUL;
        break;
      case INT8:
        opcode = Opcodes.LMUL;
        break;
      case FLOAT4:
        opcode = Opcodes.FMUL;
        break;
      case FLOAT8:
        opcode = Opcodes.DMUL;
        break;
      default:
        throw new RuntimeException("Plus does not support:" + evalNode.getValueType().getType());
      }

      context.evalMethod.visitInsn(opcode);

      return null;
    }

    @Override
    public Object visitDivide(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {
      super.visitDivide(context, evalNode, stack);

      int opcode;
      switch (evalNode.getValueType().getType()) {
      case INT4:
        opcode = Opcodes.IDIV;
        break;
      case INT8:
        opcode = Opcodes.LDIV;
        break;
      case FLOAT4:
        opcode = Opcodes.FDIV;
        break;
      case FLOAT8:
        opcode = Opcodes.DDIV;
        break;
      default:
        throw new RuntimeException("Plus does not support:" + evalNode.getValueType().getType());
      }

      context.evalMethod.visitInsn(opcode);

      return null;
    }

    @Override
    public Object visitModular(CodeGenContext context, BinaryEval evalNode, Stack<EvalNode> stack) {

      super.visitModular(context, evalNode, stack);

      int opcode;
      switch (evalNode.getValueType().getType()) {
      case INT4:
        opcode = Opcodes.IREM;
        break;
      case INT8:
        opcode = Opcodes.LREM;
        break;
      case FLOAT4:
        opcode = Opcodes.FREM;
        break;
      case FLOAT8:
        opcode = Opcodes.DREM;
        break;
      default:
        throw new RuntimeException("Plus does not support:" + evalNode.getValueType().getType());
      }

      context.evalMethod.visitInsn(opcode);

      return null;
    }

    public Object visitConst(CodeGenContext context, ConstEval evalNode, Stack<EvalNode> stack) {
      switch (evalNode.getValueType().getType()) {
      case INT2:
      case INT4:
        context.evalMethod.visitLdcInsn(evalNode.getValue().asInt4());
      }
      return null;
    }

    @Override
    public Object visitCast(CodeGenContext context, CastEval signedEval, Stack<EvalNode> stack) {
      super.visitCast(context, signedEval, stack);

      TajoDataTypes.Type srcType = signedEval.getOperand().getValueType().getType();
      TajoDataTypes.Type targetType = signedEval.getValueType().getType();
      CodeGenUtil.insertCastInst(context.evalMethod, srcType, targetType);

      return null;
    }
  }

  @Test
  public void testGenerateCodeFromQuery() throws InvalidStatementException, PlanningException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    ExprCodeGenerator generator = new ExprCodeGenerator();

    Target [] targets = getRawTargets("select 5 + 2 * 3 % 6;", true);
    long start = System.currentTimeMillis();
    EvalGen code = generator.generate(targets[0].getEvalTree());
    long end = System.currentTimeMillis();
    System.out.println(code.eval(1,1));
    long execute = System.currentTimeMillis();

    System.out.println(end - start + " msec");
    System.out.println(execute - end + " execute");
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

  static class MyClassLoader extends ClassLoader {
    public Class defineClass(String name, byte[] b) {
      return defineClass(name, b, 0, b.length);
    }
  }
}
