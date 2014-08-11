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
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.engine.planner.BasicLogicalPlanVisitor;
import org.apache.tajo.engine.planner.LogicalPlan;

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

  public static class EvalTreeProtoBuilderContext {
    private int seqId = 0;
    private Map<EvalNode, Integer> idMap = Maps.newHashMap();
    private PlanProto.EvalTree.Builder evalTreeBuilder = PlanProto.EvalTree.newBuilder();
  }

  public static class EvalTreeProtoDeserializer {
    public EvalNode deserialize(PlanProto.EvalTree tree) {
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
          current = new BinaryEval(type, lhs, rhs);
          evalNodeMap.put(protoNode.getId(), current);
        } else {
          throw new RuntimeException("Unknown EvalType: " + type.name());
        }
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

    public EvalNode visitUnaryEval(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, UnaryEval unaryEval) {
      // visiting childs
      stack.push(unaryEval);
      super.visitUnaryEval(context, stack, unaryEval);
      stack.pop();

      // register childs
      int [] childIds = registerGetChildIds(context, unaryEval);
      PlanProto.UnaryEval.Builder unaryBuilder = PlanProto.UnaryEval.newBuilder();
      unaryBuilder.setChildId(childIds[0]);
      if (unaryEval.getType() == EvalType.IS_NULL) {
        IsNullEval isNullEval = (IsNullEval) unaryEval;
        unaryBuilder.setNegative(isNullEval.isNot());
      } else if (unaryEval.getType() == EvalType.SIGNED) {
        SignedEval signedEval = (SignedEval) unaryEval;
        unaryBuilder.setNegative(signedEval.isNegative());
      } else if (unaryEval.getType() == EvalType.CAST) {
        CastEval castEval = (CastEval) unaryEval;
        unaryBuilder.setCastingType(castEval.getValueType());
      }

      // register itself
      int selfId = registerAndGetId(context, unaryEval);

      // build node itself
      PlanProto.EvalNode.Builder nodeBuilder = PlanProto.EvalNode.newBuilder();
      nodeBuilder.setId(selfId);
      nodeBuilder.setDataType(unaryEval.getValueType());
      nodeBuilder.setType(PlanProto.EvalType.valueOf(unaryEval.getType().name()));
      nodeBuilder.setUnary(unaryBuilder);

      // add node to eval tree
      context.evalTreeBuilder.addNodes(nodeBuilder);
      return unaryEval;
    }

    public EvalNode visitBinaryEval(EvalTreeProtoBuilderContext context, Stack<EvalNode> stack, BinaryEval binaryEval) {

      // visiting childs
      stack.push(binaryEval);
      super.visitBinaryEval(context, stack, binaryEval);
      stack.pop();

      // register childs
      int [] childIds = registerGetChildIds(context, binaryEval);
      PlanProto.BinaryEval.Builder binaryBuilder = PlanProto.BinaryEval.newBuilder();
      binaryBuilder.setLhsId(childIds[0]);
      binaryBuilder.setRhsId(childIds[1]);

      // register itself
      int selfId = registerAndGetId(context, binaryEval);

      // build node itself
      PlanProto.EvalNode.Builder nodeBuilder = PlanProto.EvalNode.newBuilder();
      nodeBuilder.setId(selfId);
      nodeBuilder.setDataType(binaryEval.getValueType());
      nodeBuilder.setType(PlanProto.EvalType.valueOf(binaryEval.getType().name()));
      nodeBuilder.setBinary(binaryBuilder);

      // add node to eval tree
      context.evalTreeBuilder.addNodes(nodeBuilder);
      return binaryEval;
    }
  }

  public static Datum deserialize(PlanProto.Datum datum) {
    switch (datum.getType()) {
    case BOOLEAN:
      return DatumFactory.createBool(datum.getBoolean());
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
    case CHAR:
    case VARCHAR:
    case TEXT:
      return DatumFactory.createText(datum.getText());
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
      builder.setInt4(datum.asInt4());
      break;
    case INT8:
      builder.setInt8(datum.asInt4());
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
