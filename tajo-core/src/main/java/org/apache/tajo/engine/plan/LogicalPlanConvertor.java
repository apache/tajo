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

package org.apache.tajo.engine.plan;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.TimestampDatum;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.util.datetime.DateTimeUtil;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.Stack;

public class LogicalPlanConvertor {
  public PlanProto.LogicalPlan convert(LogicalPlan plan) {
    PlanProto.LogicalPlan.Builder builder = PlanProto.LogicalPlan.newBuilder();

    return null;
  }



  public static class PlanProtoBuilder extends BasicLogicalPlanVisitor {

  }

  public static PlanProto.EvalTree serialize(EvalNode evalNode) {
    EvalTreeProtoBuilderContext context = new EvalTreeProtoBuilderContext();
    EvalTreeProtoSerializer serializer = new EvalTreeProtoSerializer();
    serializer.visit(context, evalNode, new Stack<EvalNode>());
    return context.evalTreeBuilder.build();
  }

  public static EvalNode deserialize(PlanProto.EvalTree evalTree) {
    return EvalTreeProtoDeserializer.deserialize(evalTree);
  }

  public static class EvalTreeProtoBuilderContext {
    private int seqId = 0;
    private Map<EvalNode, Integer> idMap = Maps.newHashMap();
    private PlanProto.EvalTree.Builder evalTreeBuilder = PlanProto.EvalTree.newBuilder();
  }

  public static class EvalTreeProtoDeserializer {

    public static EvalNode deserialize(PlanProto.EvalTree tree) {
      SortedMap<Integer, PlanProto.EvalNode> protoMap = Maps.newTreeMap();
      Map<Integer, EvalNode> evalNodeMap = Maps.newHashMap();

      for (PlanProto.EvalNode node : tree.getNodesList()) {
        protoMap.put(node.getId(), node);
      }

      EvalNode current = null;

      Iterator<Map.Entry<Integer, PlanProto.EvalNode>> it = protoMap.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<Integer, PlanProto.EvalNode> entry = it.next();

        PlanProto.EvalNode protoNode = entry.getValue();

        EvalType type = EvalType.valueOf(protoNode.getType().name());

        if (EvalType.isUnaryOperator(type)) {
          PlanProto.UnaryEval unaryProto = protoNode.getUnary();
          EvalNode child = evalNodeMap.get(unaryProto.getChildId());

          switch (type) {
          case NOT:
            current = new NotEval(child);
            break;
          case IS_NULL:
            current = new IsNullEval(unaryProto.getNegative(), child);
            break;
          case CAST:
            current = new CastEval(child, unaryProto.getCastingType());
            break;
          case SIGNED:
            current = new SignedEval(unaryProto.getNegative(), child);
            break;
          default:
            throw new RuntimeException("Unknown EvalType: " + type.name());
          }

        } else if (EvalType.isBinaryOperator(type)) {
          PlanProto.BinaryEval binProto = protoNode.getBinary();
          EvalNode lhs = evalNodeMap.get(binProto.getLhsId());
          EvalNode rhs = evalNodeMap.get(binProto.getRhsId());

          if (type == EvalType.IN) {
            current = new InEval(lhs, (RowConstantEval) rhs, binProto.getNegative());
          } else {
            current = new BinaryEval(type, lhs, rhs);
          }

        } else if (type == EvalType.CONST) {
          PlanProto.ConstEval constProto = protoNode.getConst();
          current = new ConstEval(LogicalPlanConvertor.deserialize(constProto.getValue()));

        } else if (type == EvalType.ROW_CONSTANT) {
          PlanProto.RowConstEval rowConstProto = protoNode.getRowConst();
          Datum [] values = new Datum[rowConstProto.getValuesCount()];
          for (int i = 0; i < rowConstProto.getValuesCount(); i++) {
            values[i] = LogicalPlanConvertor.deserialize(rowConstProto.getValues(i));
          }
          current = new RowConstantEval(values);

        } else if (type == EvalType.FIELD) {
          CatalogProtos.ColumnProto columnProto = protoNode.getField();
          current = new FieldEval(new Column(columnProto));

        } else if (EvalType.isFunction(type)) {
          PlanProto.FunctionEval funcProto = protoNode.getFunction();

          EvalNode [] params = new EvalNode[funcProto.getParamIdsCount()];
          for (int i = 0; i < funcProto.getParamIdsCount(); i++) {
            params[i] = evalNodeMap.get(funcProto.getParamIds(i));
          }

          FunctionDesc funcDesc = null;
          try {
            funcDesc = new FunctionDesc(funcProto.getFuncion());
            if (type == EvalType.FUNCTION) {
              GeneralFunction instance = (GeneralFunction) funcDesc.newInstance();
              current = new GeneralFunctionEval(new FunctionDesc(funcProto.getFuncion()), instance, params);
            } else if (type == EvalType.AGG_FUNCTION || type == EvalType.WINDOW_FUNCTION) {
              AggFunction instance = (AggFunction) funcDesc.newInstance();
              if (type == EvalType.AGG_FUNCTION) {
                current = new AggregationFunctionCallEval(new FunctionDesc(funcProto.getFuncion()), instance, params);
              } else {
                current = new WindowFunctionEval(new FunctionDesc(funcProto.getFuncion()), instance, params, null);
              }
            }
          } catch (ClassNotFoundException cnfe) {
            throw new NoSuchFunctionException(funcDesc.getSignature(), funcDesc.getParamTypes());
          } catch (InternalException ie) {
            throw new NoSuchFunctionException(funcDesc.getSignature(), funcDesc.getParamTypes());
          }
        } else {
          throw new RuntimeException("Unknown EvalType: " + type.name());
        }

        evalNodeMap.put(protoNode.getId(), current);
      }

      return current;
    }
  }

  public static class EvalTreeProtoSerializer extends SimpleEvalNodeVisitor<EvalTreeProtoBuilderContext> {

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
        if (context.idMap.containsKey(evalNode.getExpr(i))) {
          childIds[i] = context.idMap.get(evalNode.getExpr(i));
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
        rowConstBuilder.addValues(LogicalPlanConvertor.serialize(d));
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

    public EvalNode visitFuncCall(EvalTreeProtoBuilderContext context, FunctionEval function, Stack<EvalNode> stack) {
      // visiting and registering childs
      super.visitFuncCall(context, function, stack);
      int [] childIds = registerGetChildIds(context, function);

      // building itself
      PlanProto.FunctionEval.Builder funcBuilder = PlanProto.FunctionEval.newBuilder();
      funcBuilder.setFuncion(function.getFuncDesc().getProto());
      for (int i = 0; i < childIds.length; i++) {
        funcBuilder.addParamIds(i);
      }

      // registering itself and building EvalNode
      int selfId = registerAndGetId(context, function);
      PlanProto.EvalNode.Builder builder = createEvalBuilder(selfId, function);
      builder.setFunction(funcBuilder);
      context.evalTreeBuilder.addNodes(builder);

      return function;
    }
  }

  public static Datum deserialize(PlanProto.Datum datum) {
    switch (datum.getType()) {
    case BOOLEAN:
      return DatumFactory.createBool(datum.getBoolean());
    case CHAR:
      return DatumFactory.createChar(datum.getText());
    case INT1:
    case INT2:
      return DatumFactory.createInt2((short) datum.getInt4());
    case INT4:
      return DatumFactory.createInt4(datum.getInt4());
    case INT8:
      return DatumFactory.createInt8(datum.getInt8());
    case FLOAT4:
      return DatumFactory.createFloat4(datum.getFloat4());
    case FLOAT8:
      return DatumFactory.createFloat8(datum.getFloat8());
    case VARCHAR:
    case TEXT:
      return DatumFactory.createText(datum.getText());
    case TIMESTAMP:
      return new TimestampDatum(datum.getInt8());
    case DATE:
      return DatumFactory.createDate(datum.getInt4());
    case TIME:
      return DatumFactory.createTime(datum.getInt8());
    case BINARY:
    case BLOB:
      return DatumFactory.createBlob(datum.getBlob().toByteArray());
    default:
      throw new RuntimeException("Unknown data type: " + datum.getType().name());
    }
  }

  public static PlanProto.Datum serialize(Datum datum) {
    PlanProto.Datum.Builder builder = PlanProto.Datum.newBuilder();

    builder.setType(datum.type());

    switch (datum.type()) {
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
    default:
      throw new RuntimeException("Unknown data type: " + datum.type().name());
    }

    return builder.build();
  }
}
