/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.jdbc;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedDataTypeException;
import org.apache.tajo.plan.expr.*;
import org.apache.tajo.util.Pair;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Stack;

/**
 * A generator to build a SQL representation from a sql expression
 */
public class SQLExpressionGenerator extends SimpleEvalNodeVisitor<SQLExpressionGenerator.Context> {
  final private DatabaseMetaData dbMetaData;

  private final String LITERAL_QUOTE = "'";
  @SuppressWarnings("unused")
  private final String DEFAULT_LITERAL_QUOTE = "'";
  @SuppressWarnings("unused")
  private String IDENTIFIER_QUOTE = "\"";
  private final String DEFAULT_IDENTIFIER_QUOTE = "\"";

  public SQLExpressionGenerator(DatabaseMetaData dbMetaData) {
    this.dbMetaData = dbMetaData;
    initDatabaseDependentSQLRepr();
  }

  private void initDatabaseDependentSQLRepr() {
    String quoteStr = null;
    try {
      quoteStr = dbMetaData.getIdentifierQuoteString();
    } catch (SQLException e) {
    }
    this.IDENTIFIER_QUOTE = quoteStr != null ? quoteStr : DEFAULT_IDENTIFIER_QUOTE;
  }

  public String quote(String text) {
    return LITERAL_QUOTE + text + LITERAL_QUOTE;
  }

  public String generate(EvalNode node) {
    Context context = new Context();
    visit(context, node, new Stack<EvalNode>());
    return context.sb.toString();
  }

  public static class Context {
    StringBuilder sb = new StringBuilder();

    public void put(String text) {
      sb.append(" ").append(text).append(" ");
    }
  }

  protected EvalNode visitBinaryEval(Context context, Stack<EvalNode> stack, BinaryEval binaryEval) {
    stack.push(binaryEval);
    visit(context, binaryEval.getLeftExpr(), stack);
    context.sb.append(convertBinOperatorToSQLRepr(binaryEval.getType())).append(" ");
    visit(context, binaryEval.getRightExpr(), stack);
    stack.pop();
    return binaryEval;
  }

  protected EvalNode visitUnaryEval(Context context, UnaryEval unary, Stack<EvalNode> stack) {

    switch (unary.getType()) {
    case NOT:
      context.sb.append("NOT ");
      super.visitUnaryEval(context, unary, stack);
      break;
    case SIGNED:
      SignedEval signed = (SignedEval) unary;
      if (signed.isNegative()) {
        context.sb.append("-");
      }
      super.visitUnaryEval(context, unary, stack);
      break;
    case IS_NULL:
      super.visitUnaryEval(context, unary, stack);

      IsNullEval isNull = (IsNullEval) unary;
      if (isNull.isNot()) {
        context.sb.append("IS NOT NULL ");
      } else {
        context.sb.append("IS NULL ");
      }
      break;

    case CAST:
      super.visitUnaryEval(context, unary, stack);
      context.sb.append(" AS ").append(convertTajoTypeToSQLType(unary.getValueType()));
    }
    return unary;
  }

  protected EvalNode visitRowConstant(Context context, RowConstantEval row, Stack<EvalNode> stack) {
    StringBuilder sb = new StringBuilder("(");

    boolean first = true;
    for (Datum d : row.getValues()) {
      if (!first) {
        sb.append(",");
        first = false;
      }
      sb.append(convertLiteralToSQLRepr(d));
    }

    sb.append(")");

    context.put(sb.toString());

    return row;
  }

  @Override
  protected EvalNode visitField(Context context, FieldEval field, Stack<EvalNode> stack) {
    // strip the database name
    String tableName;
    if (CatalogUtil.isSimpleIdentifier(field.getQualifier())) {
      tableName = field.getQualifier();
    } else {
      tableName = CatalogUtil.extractSimpleName(field.getQualifier());
    }

    context.put(CatalogUtil.buildFQName(tableName, field.getColumnName()));
    return field;
  }

  @Override
  protected EvalNode visitConst(Context context, ConstEval constant, Stack<EvalNode> stack) {
    context.sb.append(convertLiteralToSQLRepr(constant.getValue())).append(" ");
    return constant;
  }

  /**
   * convert Tajo literal into SQL representation
   *
   * @param d Datum
   */
  public String convertLiteralToSQLRepr(Datum d) {
    switch (d.type()) {
    case INT1:
    case INT2:
    case INT4:
    case INT8:
    case FLOAT4:
    case FLOAT8:
    case NUMERIC:
      return d.asChars();

    case TEXT:
    case VARCHAR:
    case CHAR:
      return quote(d.asChars());

    case DATE:
      return "DATE " + quote(d.asChars());

    case TIME:
      return "TIME " + quote(d.asChars());

    case TIMESTAMP:
      return "TIMESTAMP " + quote(d.asChars());

    case NULL_TYPE:
      return "NULL";

    default:
      throw new TajoRuntimeException(new UnsupportedDataTypeException(d.type().name()));
    }
  }

  /**
   * Convert Tajo DataType into SQL DataType
   *
   * @param dataType Tajo DataType
   * @return SQL DataType
   */
  public String convertTajoTypeToSQLType(DataType dataType) {
    switch (dataType.getType()) {
    case INT1:
      return "TINYINT";
    case INT2:
      return "SMALLINT";
    case INT4:
      return "INTEGER";
    case INT8:
      return "BIGINT";
    case FLOAT4:
      return "FLOAT";
    case FLOAT8:
      return "DOUBLE";
    default:
      return dataType.getType().name();
    }
  }

  /**
   * Convert EvalType the operator notation into SQL notation
   *
   * @param op EvalType
   * @return SQL representation
   */
  public String convertBinOperatorToSQLRepr(EvalType op) {
    return op.getOperatorName();
  }
}
