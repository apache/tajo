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

package org.apache.tajo.engine.plan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.plan.proto.PlanProto;

import java.util.Map;
import java.util.Stack;

public class EvalTreeProtoSerializer
    extends SimpleEvalNodeVisitor<EvalTreeProtoSerializer.EvalTreeProtoBuilderContext> {

  private static final EvalTreeProtoSerializer instance;

  static {
    instance = new EvalTreeProtoSerializer();
  }

  public static class EvalTreeProtoBuilderContext {
    private int seqId = 0;
    private Map<EvalNode, Integer> idMap = Maps.newHashMap();
    private PlanProto.EvalTree.Builder evalTreeBuilder = PlanProto.EvalTree.newBuilder();
  }

  public static PlanProto.EvalTree serialize(EvalNode evalNode) {
    EvalTreeProtoSerializer.EvalTreeProtoBuilderContext context =
        new EvalTreeProtoSerializer.EvalTreeProtoBuilderContext();
    instance.visit(context, evalNode, new Stack<EvalNode>());
    return context.evalTreeBuilder.build();
  }

  private int registerAndGetId(EvalTreeProtoBuilderContext context, EvalNode evalNode) {
    int selfId;
    if (context.idMap.containsKey(evalNode)) {
      selfId = context.idMap.get(evalNode);
    } else {
      selfId = context.seqId++;
      context.idMap.put(evalNode, selfId);
    }

    return selfId;
  }

  private int [] registerGetChildIds(EvalTreeProtoBuilderContext context, EvalNode evalNode) {
    int [] childIds = new int[evalNode.childNum()];
    for (int i = 0; i < evalNode.childNum(); i++) {
      if (context.idMap.containsKey(evalNode.getChild(i))) {
        childIds[i] = context.idMap.get(evalNode.getChild(i));
      } else {
        childIds[i] = context.seqId++;
      }
    }
    return childIds;
  }

  private PlanProto.EvalNode.Builder createEvalBuilder(int id, EvalNode eval) {
    PlanProto.EvalNode.Builder nodeBuilder = PlanProto.EvalNode.newBuilder();
    nodeBuilder.setId(id);
    nodeBuilder.setDataType(eval.getValueType());
    nodeBuilder.setType(PlanProto.EvalType.valueOf(eval.getType().name()));
    return nodeBuilder;
  }

  @Override
  public EvalNode visitUnaryEval(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, UnaryEval unary) {
    // visiting and registering childs
    super.visitUnaryEval(context, stack, unary);
    int [] childIds = registerGetChildIds(context, unary);

    // building itself
    PlanProto.UnaryEval.Builder unaryBuilder = PlanProto.UnaryEval.newBuilder();
    unaryBuilder.setChildId(childIds[0]);
    if (unary.getType() == EvalType.IS_NULL) {
      IsNullEval isNullEval = (IsNullEval) unary;
      unaryBuilder.setNegative(isNullEval.isNot());
    } else if (unary.getType() == EvalType.SIGNED) {
      SignedEval signedEval = (SignedEval) unary;
      unaryBuilder.setNegative(signedEval.isNegative());
    } else if (unary.getType() == EvalType.CAST) {
      CastEval castEval = (CastEval) unary;
      unaryBuilder.setCastingType(castEval.getValueType());
    }

    // registering itself and building EvalNode
    int selfId = registerAndGetId(context, unary);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, unary);
    builder.setUnary(unaryBuilder);
    context.evalTreeBuilder.addNodes(builder);
    return unary;
  }

  @Override
  public EvalNode visitBinaryEval(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, BinaryEval binary) {
    // visiting and registering childs
    super.visitBinaryEval(context, stack, binary);
    int [] childIds = registerGetChildIds(context, binary);

    // building itself
    PlanProto.BinaryEval.Builder binaryBuilder = PlanProto.BinaryEval.newBuilder();
    binaryBuilder.setLhsId(childIds[0]);
    binaryBuilder.setRhsId(childIds[1]);

    // registering itself and building EvalNode
    int selfId = registerAndGetId(context, binary);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, binary);
    builder.setBinary(binaryBuilder);
    context.evalTreeBuilder.addNodes(builder);
    return binary;
  }

  @Override
  public EvalNode visitConst(EvalTreeProtoBuilderContext context, ConstEval constant, Stack<EvalNode> stack) {
    int selfId = registerAndGetId(context, constant);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, constant);
    builder.setConst(PlanProto.ConstEval.newBuilder().setValue(serialize(constant.getValue())));
    context.evalTreeBuilder.addNodes(builder);
    return constant;
  }

  @Override
  public EvalNode visitRowConstant(EvalTreeProtoBuilderContext context, RowConstantEval rowConst,
                                   Stack<EvalNode> stack) {

    PlanProto.RowConstEval.Builder rowConstBuilder = PlanProto.RowConstEval.newBuilder();
    for (Datum d : rowConst.getValues()) {
      rowConstBuilder.addValues(serialize(d));
    }

    int selfId = registerAndGetId(context, rowConst);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, rowConst);
    builder.setRowConst(rowConstBuilder);
    context.evalTreeBuilder.addNodes(builder);
    return rowConst;
  }

  public EvalNode visitField(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, FieldEval field) {
    int selfId = registerAndGetId(context, field);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, field);
    builder.setField(field.getColumnRef().getProto());
    context.evalTreeBuilder.addNodes(builder);
    return field;
  }

  public EvalNode visitBetween(EvalTreeProtoBuilderContext context, BetweenPredicateEval between,
                               Stack<EvalNode> stack) {
    // visiting and registering childs
    super.visitBetween(context, between, stack);
    int [] childIds = registerGetChildIds(context, between);
    Preconditions.checkState(childIds.length == 3, "Between must have three childs, but there are " + childIds.length
        + " child nodes");

    // building itself
    PlanProto.BetweenEval.Builder betweenBuilder = PlanProto.BetweenEval.newBuilder();
    betweenBuilder.setNegative(between.isNot());
    betweenBuilder.setSymmetric(between.isSymmetric());
    betweenBuilder.setPredicand(childIds[0]);
    betweenBuilder.setBegin(childIds[1]);
    betweenBuilder.setEnd(childIds[2]);

    int selfId = registerAndGetId(context, between);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, between);
    builder.setBetween(betweenBuilder);
    context.evalTreeBuilder.addNodes(builder);
    return between;
  }

  public EvalNode visitCaseWhen(EvalTreeProtoBuilderContext context, CaseWhenEval caseWhen, Stack<EvalNode> stack) {
    // visiting and registering childs
    super.visitCaseWhen(context, caseWhen, stack);
    int [] childIds = registerGetChildIds(context, caseWhen);
    Preconditions.checkState(childIds.length > 0, "Case When must have at least one child, but there is no child");

    // building itself
    PlanProto.CaseWhenEval.Builder caseWhenBuilder = PlanProto.CaseWhenEval.newBuilder();
    int ifCondsNum = childIds.length - (caseWhen.hasElse() ? 1 : 0);
    for (int i = 0; i < ifCondsNum; i++) {
      caseWhenBuilder.addIfConds(childIds[i]);
    }
    if (caseWhen.hasElse()) {
      caseWhenBuilder.setElse(childIds[childIds.length - 1]);
    }

    // registering itself and building EvalNode
    int selfId = registerAndGetId(context, caseWhen);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, caseWhen);
    builder.setCasewhen(caseWhenBuilder);
    context.evalTreeBuilder.addNodes(builder);

    return caseWhen;
  }

  public EvalNode visitIfThen(EvalTreeProtoBuilderContext context, CaseWhenEval.IfThenEval ifCond,
                              Stack<EvalNode> stack) {
    // visiting and registering childs
    super.visitIfThen(context, ifCond, stack);
    int [] childIds = registerGetChildIds(context, ifCond);

    // building itself
    PlanProto.IfCondEval.Builder ifCondBuilder = PlanProto.IfCondEval.newBuilder();
    ifCondBuilder.setCondition(childIds[0]);
    ifCondBuilder.setThen(childIds[1]);

    // registering itself and building EvalNode
    int selfId = registerAndGetId(context, ifCond);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, ifCond);
    builder.setIfCond(ifCondBuilder);
    context.evalTreeBuilder.addNodes(builder);

    return ifCond;
  }

  public EvalNode visitFuncCall(EvalTreeProtoBuilderContext context, FunctionEval function, Stack<EvalNode> stack) {
    // visiting and registering childs
    super.visitFuncCall(context, function, stack);
    int [] childIds = registerGetChildIds(context, function);

    // building itself
    PlanProto.FunctionEval.Builder funcBuilder = PlanProto.FunctionEval.newBuilder();
    funcBuilder.setFuncion(function.getFuncDesc().getProto());
    for (int i = 0; i < childIds.length; i++) {
      funcBuilder.addParamIds(childIds[i]);
    }

    // registering itself and building EvalNode
    int selfId = registerAndGetId(context, function);
    PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, function);
    builder.setFunction(funcBuilder);
    context.evalTreeBuilder.addNodes(builder);

    return function;
  }

  public static PlanProto.Datum serialize(Datum datum) {
    PlanProto.Datum.Builder builder = PlanProto.Datum.newBuilder();

    builder.setType(datum.type());

    switch (datum.type()) {
    case NULL_TYPE:
      break;
    case BOOLEAN:
      builder.setBoolean(datum.asBool());
      break;
    case INT1:
    case INT2:
    case INT4:
    case DATE:
      builder.setInt4(datum.asInt4());
      break;
    case INT8:
    case TIMESTAMP:
    case TIME:
      builder.setInt8(datum.asInt8());
      break;
    case FLOAT4:
      builder.setFloat4(datum.asFloat4());
      break;
    case FLOAT8:
      builder.setFloat8(datum.asFloat8());
      break;
    case CHAR:
    case VARCHAR:
    case TEXT:
      builder.setText(datum.asChars());
      break;
    case BINARY:
    case BLOB:
      builder.setBlob(ByteString.copyFrom(datum.asByteArray()));
      break;
    case INTERVAL:
      IntervalDatum interval = (IntervalDatum) datum;
      PlanProto.Interval.Builder intervalBuilder = PlanProto.Interval.newBuilder();
      intervalBuilder.setMonth(interval.getMonths());
      intervalBuilder.setMsec(interval.getMilliSeconds());
      builder.setInterval(intervalBuilder);
      break;
    default:
      throw new RuntimeException("Unknown data type: " + datum.type().name());
    }

    return builder.build();
  }
}
