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

package tajo.engine.parser;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import tajo.engine.eval.EvalNode;
import tajo.engine.eval.InvalidEvalException;

public class ParseUtil {
  private static final Log LOG = LogFactory.getLog(ParseUtil.class);

  public static boolean isConstant(final Tree tree) {
    switch (tree.getType()) {
      case SQLParser.NUMBER:
      case SQLParser.REAL:
      case SQLParser.Character_String_Literal:
        return true;
      default:
        return false;
    }
  }

  public static EvalNode.Type getTypeByParseCode(int parseCode) {
    switch(parseCode) {
      case SQLParser.AND:
        return EvalNode.Type.AND;
      case SQLParser.OR:
        return EvalNode.Type.OR;
      case SQLParser.LIKE:
        return EvalNode.Type.LIKE;
      case SQLParser.EQUAL:
        return EvalNode.Type.EQUAL;
      case SQLParser.NOT_EQUAL:
        return EvalNode.Type.NOT_EQUAL;
      case SQLParser.LTH:
        return EvalNode.Type.LTH;
      case SQLParser.LEQ:
        return EvalNode.Type.LEQ;
      case SQLParser.GTH:
        return EvalNode.Type.GTH;
      case SQLParser.GEQ:
        return EvalNode.Type.GEQ;
      case SQLParser.NOT:
        return EvalNode.Type.NOT;
      case SQLParser.PLUS:
        return EvalNode.Type.PLUS;
      case SQLParser.MINUS:
        return EvalNode.Type.MINUS;
      case SQLParser.MULTIPLY:
        return EvalNode.Type.MULTIPLY;
      case SQLParser.DIVIDE:
        return EvalNode.Type.DIVIDE;
      case SQLParser.MODULAR:
        return EvalNode.Type.MODULAR;
      default: throw new InvalidEvalException("We does not support " + parseCode + " type AST yet");
    }
  }
}
