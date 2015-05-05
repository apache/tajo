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

package org.apache.tajo.plan.serder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.tajo.algebra.WindowSpec.WindowFrameEndBoundType;
import org.apache.tajo.algebra.WindowSpec.WindowFrameStartBoundType;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.AnyDatum;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.logical.WindowSpec;
import org.apache.tajo.plan.serder.PlanProto.WinFunctionEvalSpec;
import org.apache.tajo.plan.serder.PlanProto.WinFunctionEvalSpec.WindowFrame;
import org.apache.tajo.util.ProtoUtil;

import java.util.Map;
import java.util.Stack;

/**
 * It traverses an eval tree consisting of a number of {@link org.apache.tajo.plan.expr.EvalNode}
 * in a postfix traverse order. The postfix traverse order guarantees that all child nodes of some node N
 * were already visited when the node N is visited. This manner makes tree serialization possible in a simple logic.
 */
public class EvalNodeSerializer
    extends SimpleEvalNodeVisitor<EvalNodeSerializer.EvalTreeProtoBuilderContext> {

  private static final EvalNodeSerializer instance;

  static {
    instance = new EvalNodeSerializer();
  }

  public static class EvalTreeProtoBuilderContext {
    private int seqId = 0;
    private Map<EvalNode, Integer> idMap = Maps.newHashMap();
    private PlanProto.EvalNodeTree.Builder treeBuilder = PlanProto.EvalNodeTree.newBuilder();
  }

  public static PlanProto.EvalNodeTree serialize(EvalNode evalNode) {
    EvalNodeSerializer.EvalTreeProtoBuilderContext context =
        new EvalNodeSerializer.EvalTreeProtoBuilderContext();
    instance.visit(context, evalNode, new Stack<EvalNode>());
    return context.treeBuilder.build();
  }

  /**
   * Return child's serialization IDs. Usually, 0 is used for a child id of unary node or left child of
   * binary node. 1 is used for right child of binary node. Between will use 0 as predicand, 1 as begin, and 2 as
   * end eval node. For more detail, you should refer to each EvalNode implementation.
   *
   * @param context Context
   * @param evalNode EvalNode
   * @return The array of IDs which points to stored EvalNode.
   * @see org.apache.tajo.plan.expr.EvalNode
   */
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

  private PlanProto.EvalNode.Builder createEvalBuilder(EvalTreeProtoBuilderContext context, EvalNode node) {
    int sid; // serialization sequence id
    if (context.idMap.containsKey(node)) {
      sid = context.idMap.get(node);
    } else {
      sid = context.seqId++;
      context.idMap.put(node, sid);
    }

    PlanProto.EvalNode.Builder nodeBuilder = PlanProto.EvalNode.newBuilder();
    nodeBuilder.setId(sid);
    nodeBuilder.setDataType(node.getValueType());
    nodeBuilder.setType(PlanProto.EvalType.valueOf(node.getType().name()));
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
      if (castEval.hasTimeZone()) {
        unaryBuilder.setTimezone(castEval.getTimezone().getID());
      }
    }

    // registering itself and building EvalNode
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, unary);
    builder.setUnary(unaryBuilder);
    context.treeBuilder.addNodes(builder);
    return unary;
  }

  @Override
  public EvalNode visitBinaryEval(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, BinaryEval binary) {
    // visiting and registering childs
    super.visitBinaryEval(context, stack, binary);
    int [] childIds = registerGetChildIds(context, binary);

    // registering itself and building EvalNode
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, binary);

    // building itself
    PlanProto.BinaryEval.Builder binaryBuilder = PlanProto.BinaryEval.newBuilder();
    binaryBuilder.setLhsId(childIds[0]);
    binaryBuilder.setRhsId(childIds[1]);

    if (binary instanceof InEval) {
      binaryBuilder.setNegative(((InEval)binary).isNot());
    } else if (binary instanceof PatternMatchPredicateEval) {
      PatternMatchPredicateEval patternMatch = (PatternMatchPredicateEval) binary;
      binaryBuilder.setNegative(patternMatch.isNot());
      builder.setPatternMatch(
          PlanProto.PatternMatchEvalSpec.newBuilder().setCaseSensitive(patternMatch.isCaseInsensitive()));
    }

    builder.setBinary(binaryBuilder);
    context.treeBuilder.addNodes(builder);
    return binary;
  }

  @Override
  public EvalNode visitConst(EvalTreeProtoBuilderContext context, ConstEval constant, Stack<EvalNode> stack) {
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, constant);
    builder.setConst(PlanProto.ConstEval.newBuilder().setValue(serialize(constant.getValue())));
    context.treeBuilder.addNodes(builder);
    return constant;
  }

  @Override
  public EvalNode visitRowConstant(EvalTreeProtoBuilderContext context, RowConstantEval rowConst,
                                   Stack<EvalNode> stack) {

    PlanProto.RowConstEval.Builder rowConstBuilder = PlanProto.RowConstEval.newBuilder();
    for (Datum d : rowConst.getValues()) {
      rowConstBuilder.addValues(serialize(d));
    }

    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, rowConst);
    builder.setRowConst(rowConstBuilder);
    context.treeBuilder.addNodes(builder);
    return rowConst;
  }

  @Override
  public EvalNode visitField(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, FieldEval field) {
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, field);
    builder.setField(field.getColumnRef().getProto());
    context.treeBuilder.addNodes(builder);
    return field;
  }

  @Override
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

    // registering itself and building EvalNode
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, between);
    builder.setBetween(betweenBuilder);
    context.treeBuilder.addNodes(builder);
    return between;
  }

  @Override
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
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, caseWhen);
    builder.setCasewhen(caseWhenBuilder);
    context.treeBuilder.addNodes(builder);

    return caseWhen;
  }

  @Override
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
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, ifCond);
    builder.setIfCond(ifCondBuilder);
    context.treeBuilder.addNodes(builder);

    return ifCond;
  }

  @Override
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
    PlanProto.EvalNode.Builder builder = createEvalBuilder(context, function);
    builder.setFunction(funcBuilder);

    if (function instanceof AggregationFunctionCallEval) {
      AggregationFunctionCallEval aggFunc = (AggregationFunctionCallEval) function;

      PlanProto.AggFunctionEvalSpec.Builder aggFunctionEvalBuilder = PlanProto.AggFunctionEvalSpec.newBuilder();
      aggFunctionEvalBuilder.setFirstPhase(aggFunc.isFirstPhase());
      aggFunctionEvalBuilder.setLastPhase(aggFunc.isLastPhase());
      if (aggFunc.hasAlias()) {
        aggFunctionEvalBuilder.setAlias(aggFunc.getAlias());
      }

      builder.setAggFunction(aggFunctionEvalBuilder);
    }


    if (function instanceof WindowFunctionEval) {
      WindowFunctionEval winFunc = (WindowFunctionEval) function;
      WinFunctionEvalSpec.Builder windowFuncBuilder = WinFunctionEvalSpec.newBuilder();

      if (winFunc.hasSortSpecs()) {
        windowFuncBuilder.addAllSortSpec(ProtoUtil.<CatalogProtos.SortSpecProto>toProtoObjects
            (winFunc.getSortSpecs()));
      }

      windowFuncBuilder.setWindowFrame(buildWindowFrame(winFunc.getWindowFrame()));
      builder.setWinFunction(windowFuncBuilder);
    }

    context.treeBuilder.addNodes(builder);
    return function;
  }

  private WindowFrame buildWindowFrame(WindowSpec.WindowFrame frame) {
    WindowFrame.Builder windowFrameBuilder = WindowFrame.newBuilder();

    WindowSpec.WindowStartBound startBound = frame.getStartBound();
    WindowSpec.WindowEndBound endBound = frame.getEndBound();

    WinFunctionEvalSpec.WindowStartBound.Builder startBoundBuilder = WinFunctionEvalSpec.WindowStartBound.newBuilder();
    startBoundBuilder.setBoundType(convertStartBoundType(startBound.getBoundType()));

    WinFunctionEvalSpec.WindowEndBound.Builder endBoundBuilder = WinFunctionEvalSpec.WindowEndBound.newBuilder();
    endBoundBuilder.setBoundType(convertEndBoundType(endBound.getBoundType()));

    windowFrameBuilder.setStartBound(startBoundBuilder);
    windowFrameBuilder.setEndBound(endBoundBuilder);

    return windowFrameBuilder.build();
  }

  private WinFunctionEvalSpec.WindowFrameStartBoundType convertStartBoundType(WindowFrameStartBoundType type) {
    if (type == WindowFrameStartBoundType.UNBOUNDED_PRECEDING) {
      return WinFunctionEvalSpec.WindowFrameStartBoundType.S_UNBOUNDED_PRECEDING;
    } else if (type == WindowFrameStartBoundType.CURRENT_ROW) {
      return WinFunctionEvalSpec.WindowFrameStartBoundType.S_CURRENT_ROW;
    } else if (type == WindowFrameStartBoundType.PRECEDING) {
      return WinFunctionEvalSpec.WindowFrameStartBoundType.S_PRECEDING;
    } else {
      throw new IllegalStateException("Unknown Window Start Bound type: " + type.name());
    }
  }

  private WinFunctionEvalSpec.WindowFrameEndBoundType convertEndBoundType(WindowFrameEndBoundType type) {
    if (type == WindowFrameEndBoundType.UNBOUNDED_FOLLOWING) {
      return WinFunctionEvalSpec.WindowFrameEndBoundType.E_UNBOUNDED_FOLLOWING;
    } else if (type == WindowFrameEndBoundType.CURRENT_ROW) {
      return WinFunctionEvalSpec.WindowFrameEndBoundType.E_CURRENT_ROW;
    } else if (type == WindowFrameEndBoundType.FOLLOWING) {
      return WinFunctionEvalSpec.WindowFrameEndBoundType.E_FOLLOWING;
    } else {
      throw new IllegalStateException("Unknown Window End Bound type: " + type.name());
    }
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
    case ANY:
      builder.setActual(serialize(((AnyDatum)datum).getActual()));
      break;
    default:
      throw new RuntimeException("Unknown data type: " + datum.type().name());
    }

    return builder.build();
  }
}
