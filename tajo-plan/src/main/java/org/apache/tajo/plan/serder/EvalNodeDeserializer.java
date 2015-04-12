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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.WindowSpec.WindowFrameEndBoundType;
import org.apache.tajo.algebra.WindowSpec.WindowFrameStartBoundType;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.exception.NoSuchFunctionException;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionSignatureProto;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.InternalException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.plan.function.AggFunction;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.plan.logical.WindowSpec;
import org.apache.tajo.plan.serder.PlanProto.WinFunctionEvalSpec;

import java.io.IOException;
import java.util.*;

/**
 * It deserializes a serialized eval tree consisting of a number of EvalNodes.
 *
 * {@link EvalNodeSerializer} serializes an eval tree in a postfix traverse order.
 * So, this class firstly sorts all serialized eval nodes in ascending order of their sequence IDs. Then,
 * it sequentially restores each serialized node to EvalNode instance.
 *
 * @see EvalNodeSerializer
 */
public class EvalNodeDeserializer {

  public static EvalNode deserialize(OverridableConf context, EvalContext evalContext, PlanProto.EvalNodeTree tree) {
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
          current = new CastEval(context, child, unaryProto.getCastingType());
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
        case LIKE: {
          PlanProto.PatternMatchEvalSpec patternMatchProto = protoNode.getPatternMatch();
          current = new LikePredicateEval(binProto.getNegative(), lhs, (ConstEval) rhs,
              patternMatchProto.getCaseSensitive());
          break;
        }
        case REGEX: {
          PlanProto.PatternMatchEvalSpec patternMatchProto = protoNode.getPatternMatch();
          current = new RegexPredicateEval(binProto.getNegative(), lhs, (ConstEval) rhs,
              patternMatchProto.getCaseSensitive());
          break;
        }
        case SIMILAR_TO: {
          PlanProto.PatternMatchEvalSpec patternMatchProto = protoNode.getPatternMatch();
          current = new SimilarToPredicateEval(binProto.getNegative(), lhs, (ConstEval) rhs,
              patternMatchProto.getCaseSensitive());
          break;
        }

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
            current = new GeneralFunctionEval(context, funcDesc, params);
            if (evalContext != null && funcDesc.getInvocation().hasPython()) {
              evalContext.addScriptEngine(current, new PythonScriptEngine(funcDesc));
            }
          } else if (type == EvalType.AGG_FUNCTION || type == EvalType.WINDOW_FUNCTION) {
            AggFunction instance = (AggFunction) funcDesc.newInstance();
            if (type == EvalType.AGG_FUNCTION) {
              AggregationFunctionCallEval aggFunc =
                  new AggregationFunctionCallEval(new FunctionDesc(funcProto.getFuncion()), instance, params);

              PlanProto.AggFunctionEvalSpec aggFunctionProto = protoNode.getAggFunction();
              aggFunc.setIntermediatePhase(aggFunctionProto.getIntermediatePhase());
              aggFunc.setFinalPhase(aggFunctionProto.getFinalPhase());
              if (aggFunctionProto.hasAlias()) {
                aggFunc.setAlias(aggFunctionProto.getAlias());
              }
              current = aggFunc;

            } else {
              WinFunctionEvalSpec windowFuncProto = protoNode.getWinFunction();

              WindowFunctionEval winFunc =
                  new WindowFunctionEval(new FunctionDesc(funcProto.getFuncion()), instance, params,
                      convertWindowFrame(windowFuncProto.getWindowFrame()));

              if (windowFuncProto.getSortSpecCount() > 0) {
                SortSpec[] sortSpecs = LogicalNodeDeserializer.convertSortSpecs(windowFuncProto.getSortSpecList());
                winFunc.setSortSpecs(sortSpecs);
              }

              current = winFunc;
            }
          }
        } catch (ClassNotFoundException cnfe) {
          String functionName = "Unknown";
          DataType[] parameterTypes = new DataType[0];
          if (funcProto.getFuncion() != null && funcProto.getFuncion().getSignature() != null) {
            FunctionSignatureProto funcSignatureProto = funcProto.getFuncion().getSignature();
            
            if (funcSignatureProto.hasName()) {
              functionName = funcSignatureProto.getName();
            }
            
            parameterTypes = funcSignatureProto.getParameterTypesList().toArray(
                new DataType[funcSignatureProto.getParameterTypesCount()]);
          }
          throw new NoSuchFunctionException(functionName, parameterTypes);
        } catch (InternalException ie) {
          throw new NoSuchFunctionException(funcDesc.getFunctionName(), funcDesc.getParamTypes());
        } catch (IOException e) {
          throw new NoSuchFunctionException(e.getMessage());
        }
      } else {
        throw new RuntimeException("Unknown EvalType: " + type.name());
      }

      evalNodeMap.put(protoNode.getId(), current);
    }

    return current;
  }

  private static WindowSpec.WindowFrame convertWindowFrame(WinFunctionEvalSpec.WindowFrame windowFrame) {
    WindowFrameStartBoundType startBoundType = convertWindowStartBound(windowFrame.getStartBound().getBoundType());
    WindowSpec.WindowStartBound startBound = new WindowSpec.WindowStartBound(startBoundType);

    WindowFrameEndBoundType endBoundType = convertWindowEndBound(windowFrame.getEndBound().getBoundType());
    WindowSpec.WindowEndBound endBound = new WindowSpec.WindowEndBound(endBoundType);

    WindowSpec.WindowFrame frame = new WindowSpec.WindowFrame(startBound, endBound);
    return frame;
  }

  private static WindowFrameStartBoundType convertWindowStartBound(
      WinFunctionEvalSpec.WindowFrameStartBoundType type) {
    if (type == WinFunctionEvalSpec.WindowFrameStartBoundType.S_UNBOUNDED_PRECEDING) {
      return WindowFrameStartBoundType.UNBOUNDED_PRECEDING;
    } else if (type == WinFunctionEvalSpec.WindowFrameStartBoundType.S_CURRENT_ROW) {
      return WindowFrameStartBoundType.CURRENT_ROW;
    } else if (type == WinFunctionEvalSpec.WindowFrameStartBoundType.S_PRECEDING) {
      return WindowFrameStartBoundType.PRECEDING;
    } else {
      throw new IllegalStateException("Unknown Window Start Bound type: " + type.name());
    }
  }

  private static WindowFrameEndBoundType convertWindowEndBound(
      WinFunctionEvalSpec.WindowFrameEndBoundType type) {
    if (type == WinFunctionEvalSpec.WindowFrameEndBoundType.E_UNBOUNDED_FOLLOWING) {
      return WindowFrameEndBoundType.UNBOUNDED_FOLLOWING;
    } else if (type == WinFunctionEvalSpec.WindowFrameEndBoundType.E_CURRENT_ROW) {
      return WindowFrameEndBoundType.CURRENT_ROW;
    } else if (type == WinFunctionEvalSpec.WindowFrameEndBoundType.E_FOLLOWING) {
      return WindowFrameEndBoundType.FOLLOWING;
    } else {
      throw new IllegalStateException("Unknown Window Start Bound type: " + type.name());
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
    case INTERVAL:
      return new IntervalDatum(datum.getInterval().getMonth(), datum.getInterval().getMsec());
    case NULL_TYPE:
      return NullDatum.get();
    case ANY:
      return DatumFactory.createAny(deserialize(datum.getActual()));
    default:
      throw new RuntimeException("Unknown data type: " + datum.getType().name());
    }
  }
}
