/*
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

package org.apache.tajo.plan.function.python;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.BlobDatum;
import org.apache.tajo.function.FunctionInvocation;
import org.apache.tajo.function.FunctionSignature;
import org.apache.tajo.function.FunctionSupplement;
import org.apache.tajo.function.PythonInvocationDesc;
import org.apache.tajo.util.TUtil;

import java.io.*;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class PythonScriptEngine extends TajoScriptEngine {

  private static final String LANGUAGE_NAME = "python";
  private static final Log LOG = LogFactory.getLog(PythonScriptEngine.class);

  public static Set<FunctionDesc> registerFunctions(String path, String namespace) throws IOException {

    Set<FunctionDesc> functionDescs = TUtil.newHashSet();

    String fileName = path.substring(0, path.length() - ".py".length());
    LOG.debug("Path: " + path + " FileName: " + fileName + " Namespace: " + namespace);
    InputStream in = getScriptAsStream(path);
    List<FuncInfo> functions = null;
    try {
      functions = getFunctions(in);
    } finally {
      in.close();
    }
    namespace = namespace == null ? "" : namespace + NAMESPACE_SEPARATOR;
    for(FuncInfo funcInfo : functions) {
      String alias = namespace + funcInfo.funcName;
      LOG.debug("Registering Function: " + alias);

      TajoDataTypes.DataType returnType = CatalogUtil.newSimpleDataType(TajoDataTypes.Type.valueOf(funcInfo.returnType));
      FunctionSignature signature = new FunctionSignature(CatalogProtos.FunctionType.UDF, funcInfo.funcName,
          returnType, createParamTypes(funcInfo.paramNum));
      FunctionInvocation invocation = new FunctionInvocation();
      PythonInvocationDesc invocationDesc = new PythonInvocationDesc(funcInfo.funcName, path);
      invocation.setPython(invocationDesc);
      FunctionSupplement supplement = new FunctionSupplement();
      functionDescs.add(new FunctionDesc(signature, invocation, supplement));
    }
    return functionDescs;
  }

  private static TajoDataTypes.DataType[] createParamTypes(int paramNum) {
    TajoDataTypes.DataType[] paramTypes = new TajoDataTypes.DataType[paramNum];
    for (int i = 0; i < paramNum; i++) {
      paramTypes[i] = TajoDataTypes.DataType.newBuilder().setType(TajoDataTypes.Type.ANY).build();
    }
    return paramTypes;
  }

  @Override
  protected String getScriptingLang() {
    return LANGUAGE_NAME;
  }

  private static final Pattern pSchema = Pattern.compile("^\\s*\\W+outputType.*");
  private static final Pattern pDef = Pattern.compile("^\\s*def\\s+(\\w+)\\s*.+");
  private static final String FUNC_PREFIX = "def ";
  private static final String BLOB_TYPE_STRING = TajoDataTypes.Type.BLOB.name();
  private static final String PARAM_DELIMITER = ",";
  private static final String LEFT_PAREN = "(";
  private static final String RIGHT_PAREN = ")";
  private static final String EMPTY_STR = "";

  private static class FuncInfo {
    String returnType;
    String funcName;
    int paramNum;
    int schemaLineNumber;

    public FuncInfo(String returnType, String funcName, int paramNum, int schemaLineNumber) {
      this.returnType = returnType.toUpperCase();
      this.funcName = funcName;
      this.paramNum = paramNum;
      this.schemaLineNumber = schemaLineNumber;
    }
  }

  // TODO: python parser must be improved.
  private static List<FuncInfo> getFunctions(InputStream is) throws IOException {
    List<FuncInfo> functions = TUtil.newList();
    InputStreamReader in = new InputStreamReader(is, Charset.defaultCharset());
    BufferedReader br = new BufferedReader(in);
    String line = br.readLine();
    String schemaString = null;
    int lineNumber = 1;
    int schemaLineNumber = -1;
    while (line != null) {
      if (pSchema.matcher(line).matches()) {
        int start = line.indexOf(LEFT_PAREN) + 2; //drop brackets/quotes
        int end = line.lastIndexOf(RIGHT_PAREN) - 1;
        schemaString = line.substring(start,end).trim();
        schemaLineNumber = lineNumber;
      } else if (pDef.matcher(line).matches()) {
        int nameStart = line.indexOf(FUNC_PREFIX) + FUNC_PREFIX.length();
        int nameEnd = line.indexOf(LEFT_PAREN);
        int signatureEnd = line.indexOf(RIGHT_PAREN);
        String[] params = line.substring(nameEnd+1, signatureEnd).split(PARAM_DELIMITER);
        int paramNum;
        if (params.length == 1) {
          paramNum = params[0].equals(EMPTY_STR) ? 0 : 1;
        } else {
          paramNum = params.length;
        }

        String functionName = line.substring(nameStart, nameEnd).trim();
        schemaString = schemaString == null ? BLOB_TYPE_STRING : schemaString;
        functions.add(new FuncInfo(schemaString, functionName, paramNum, schemaLineNumber));
        schemaString = null;
      }
      line = br.readLine();
      lineNumber++;
    }
    br.close();
    in.close();
    return functions;
  }
}
