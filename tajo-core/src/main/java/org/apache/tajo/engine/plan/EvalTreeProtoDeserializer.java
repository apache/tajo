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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.datum.*;
import org.apache.tajo.engine.eval.*;
import org.apache.tajo.engine.function.AggFunction;
import org.apache.tajo.engine.function.GeneralFunction;
import org.apache.tajo.engine.plan.proto.PlanProto;
import org.apache.tajo.exception.InternalException;

import java.util.*;

/**
 * It deserializes a serialized eval tree consisting of a number of EvalNodes.
 *
 * {@link org.apache.tajo.engine.plan.EvalTreeProtoSerializer} serializes an eval tree in a postfix traverse order.
 * So, this class firstly sorts all serialized eval nodes in ascending order of their sequence IDs. Then,
 * it sequentially restores each serialized node to EvalNode instance.
 *
 * @see org.apache.tajo.engine.plan.EvalTreeProtoSerializer
 */
public class EvalTreeProtoDeserializer {

  public static EvalNode deserialize(PlanProto.EvalTree tree) {
    Map<Integer, EvalNode> evalNodeMap = Maps.newHashMap();

    // sort serialized eval nodes in an ascending order of their IDs.
    List<PlanProto.EvalNode> nodeList = Lists.newArrayList(tree.getNodesList());
    Collections.sort(nodeList, new Comparator<PlanProto.EvalNode>() {
      @Override
      public int compare(PlanProto.EvalNode o1, PlanProto.EvalNode o2) {
        return o1.getId() - o2.getId();
      }
    });

    EvalNode current = null;

    // The sorted order is the same of a postfix traverse order.
    // So, it sequentially transforms each serialized node into a EvalNode instance in a postfix order of
    // the original eval tree.

    Iterator<PlanProto.EvalNode> it = nodeList.iterator();
    while (it.hasNext()) {
      PlanProto.EvalNode protoNode = it.next();

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

        switch (type) {
        case IN:
          current = new InEval(lhs, (RowConstantEval) rhs, binProto.getNegative());
          break;
        default:
          current = new BinaryEval(type, lhs, rhs);
        }

      } else if (type == EvalType.CONST) {
        PlanProto.ConstEval constProto = protoNode.getConst();
        current = new ConstEval(deserialize(constProto.getValue()));

      } else if (type == EvalType.ROW_CONSTANT) {
        PlanProto.RowConstEval rowConstProto = protoNode.getRowConst();
        Datum[] values = new Datum[rowConstProto.getValuesCount()];
        for (int i = 0; i < rowConstProto.getValuesCount(); i++) {
          values[i] = deserialize(rowConstProto.getValues(i));
        }
        current = new RowConstantEval(values);

      } else if (type == EvalType.FIELD) {
        CatalogProtos.ColumnProto columnProto = protoNode.getField();
        current = new FieldEval(new Column(columnProto));

      } else if (type == EvalType.BETWEEN) {
        PlanProto.BetweenEval betweenProto = protoNode.getBetween();
        current = new BetweenPredicateEval(betweenProto.getNegative(), betweenProto.getSymmetric(),
            evalNodeMap.get(betweenProto.getPredicand()),
            evalNodeMap.get(betweenProto.getBegin()),
            evalNodeMap.get(betweenProto.getEnd()));

      } else if (type == EvalType.CASE) {
        PlanProto.CaseWhenEval caseWhenProto = protoNode.getCasewhen();
        CaseWhenEval caseWhenEval = new CaseWhenEval();
        for (int i = 0; i < caseWhenProto.getIfCondsCount(); i++) {
          caseWhenEval.addIfCond((CaseWhenEval.IfThenEval) evalNodeMap.get(caseWhenProto.getIfConds(i)));
        }
        if (caseWhenProto.hasElse()) {
          caseWhenEval.setElseResult(evalNodeMap.get(caseWhenProto.getElse()));
        }
        current = caseWhenEval;

      } else if (type == EvalType.IF_THEN) {
        PlanProto.IfCondEval ifCondProto = protoNode.getIfCond();
        current = new CaseWhenEval.IfThenEval(evalNodeMap.get(ifCondProto.getCondition()),
            evalNodeMap.get(ifCondProto.getThen()));

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
          throw new NoSuchFunctionException(funcDesc.getFunctionName(), funcDesc.getParamTypes());
        } catch (InternalException ie) {
          throw new NoSuchFunctionException(funcDesc.getFunctionName(), funcDesc.getParamTypes());
        }
      } else {
        throw new RuntimeException("Unknown EvalType: " + type.name());
      }

      evalNodeMap.put(protoNode.getId(), current);
    }

    return current;
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
    case INTERVAL:
      return new IntervalDatum(datum.getInterval().getMonth(), datum.getInterval().getMsec());
    case NULL_TYPE:
      return NullDatum.get();
    default:
      throw new RuntimeException("Unknown data type: " + datum.getType().name());
    }
  }
}
