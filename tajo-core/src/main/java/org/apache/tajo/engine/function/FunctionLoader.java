/***
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

package org.apache.tajo.engine.function;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.FunctionDesc;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamOptionTypes;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.exception.AmbiguousFunctionException;
import org.apache.tajo.function.*;
import org.apache.tajo.plan.function.python.PythonScriptEngine;
import org.apache.tajo.util.ClassUtil;
import org.apache.tajo.util.TUtil;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tajo.catalog.proto.CatalogProtos.FunctionType.GENERAL;

public class FunctionLoader {

  private static Log LOG = LogFactory.getLog(FunctionLoader.class);
  public static final String PYTHON_FUNCTION_NAMESPACE = "python";

  /**
   * Load built-in functions
   *
   * @return
   */
  public static Map<FunctionSignature, FunctionDesc> loadBuiltinFunctions() {
    Map<FunctionSignature, FunctionDesc> map = Maps.newHashMap();

    List<FunctionDesc> dd = Lists.newArrayList();
    for (FunctionDesc f : findLegacyFunctions()) {
      map.put(f.getSignature(), f);

      if (f.getSignature().getName().equals("pow") || f.getSignature().getName().equals("pi")) {
        dd.add(f);
      }
    }

    for (FunctionDesc f : findScalarFunctions()) {
      if (map.containsKey(f.getSignature())) {
        FunctionDesc existing = map.get(f.getSignature());
        existing.getInvocation().setScalar(f.getInvocation().getScalar());
      } else {
        map.put(f.getSignature(), f);
      }
    }

    return map;
  }

  /**
   * Load functions defined by users.
   *
   * @param conf
   * @return
   * @throws IOException
   */
  public static List<FunctionDesc> loadUserDefinedFunctions(TajoConf conf)
      throws IOException {
    List<FunctionDesc> functionList = new LinkedList<>();

    String[] codePaths = conf.getStrings(TajoConf.ConfVars.PYTHON_CODE_DIR.varname);
    if (codePaths != null) {
      FileSystem localFS = FileSystem.getLocal(conf);
      for (String codePathStr : codePaths) {

        Path codePath;
        try {
          codePath = new Path(codePathStr);
        } catch (IllegalArgumentException e) {
          LOG.warn(e);
          continue;
        }

        List<Path> filePaths = TUtil.newList();
        if (localFS.isDirectory(codePath)) {
          for (FileStatus file : localFS.listStatus(codePath,
              (Path path) -> path.getName().endsWith(PythonScriptEngine.FILE_EXTENSION))) {
            filePaths.add(file.getPath());
          }
        } else {
          filePaths.add(codePath);
        }
        for (Path filePath : filePaths) {
          PythonScriptEngine.registerFunctions(filePath.toUri(), FunctionLoader.PYTHON_FUNCTION_NAMESPACE).forEach(functionList::add);
        }
      }
    }

    return functionList;
  }

  public static Set<FunctionDesc> findScalarFunctions() {
    Set<FunctionDesc> functions = Sets.newHashSet();

    Set<Method> scalarFunctions = findPublicStaticMethods("org.apache.tajo.engine.function",
        (Object object) -> ((Method) object).getAnnotation(ScalarFunction.class) != null);

    for (Method method : scalarFunctions) {
      ScalarFunction annotation = method.getAnnotation(ScalarFunction.class);
      functions.addAll(buildFunctionDescs(annotation, method));
    }

    return functions;
  }

  private static Set<Method> findPublicStaticMethods(String packageName, Predicate predicate) {
    Set<Class> found = findFunctionCollections(packageName);
    Set<Method> filtered = Sets.newHashSet();

    for (Class clazz : found) {
      for (Method method : clazz.getMethods()) {
        if (isPublicStaticMethod(method) && (predicate == null || predicate.evaluate(method))) {
          filtered.add(method);
        }
      }
    }

    return filtered;
  }

  private static boolean isPublicStaticMethod(Method method) {
    return Modifier.isPublic(method.getModifiers()) && Modifier.isStatic(method.getModifiers());
  }

  private static Set<Class> findFunctionCollections(String packageName) {
    return ClassUtil.findClasses(null, packageName, (Object object) ->
        ((Class)object).getAnnotation(FunctionCollection.class) != null);
  }

  private static Collection<FunctionDesc> buildFunctionDescs(ScalarFunction annotation, Method method) {
    List<FunctionDesc> functionDescs = Lists.newArrayList();

    FunctionInvocation invocation = new FunctionInvocation();
    invocation.setScalar(extractStaticMethodInvocation(method));
    FunctionSupplement supplement = extractSupplement(annotation);

    // primary name
    functionDescs.add(new FunctionDesc(extractSignature(annotation, null), invocation, supplement));

    // for multiple aliases
    for (String alias : annotation.synonyms()) {
      functionDescs.add(new FunctionDesc(extractSignature(annotation, alias), invocation, supplement));
    }

    return functionDescs;
  }

  private static FunctionSignature extractSignature(ScalarFunction annotation, @Nullable String alias) {
    return new FunctionSignature(
        GENERAL,
        alias != null ? alias : annotation.name(),
        CatalogUtil.newSimpleDataType(annotation.returnType()),
        CatalogUtil.newSimpleDataTypeArray(annotation.paramTypes())
    );
  }

  private static FunctionSupplement extractSupplement(ScalarFunction function) {
    return new FunctionSupplement(
            function.shortDescription(),
            function.detail(),
            function.example());
  }

  private static StaticMethodInvocationDesc extractStaticMethodInvocation(Method method) {
    Preconditions.checkArgument(Modifier.isPublic(method.getModifiers()));
    Preconditions.checkArgument(Modifier.isStatic(method.getModifiers()));

    String methodName = method.getName();
    Class returnClass = method.getReturnType();
    Class [] paramClasses = method.getParameterTypes();
    return new StaticMethodInvocationDesc(method.getDeclaringClass(), methodName, returnClass, paramClasses);
  }

  /**
   * This method finds and build FunctionDesc for the legacy function and UD(A)F system.
   *
   * @return A list of FunctionDescs
   */
  public static List<FunctionDesc> findLegacyFunctions() {
    List<FunctionDesc> sqlFuncs = new ArrayList<>();

    Set<Class> functionClasses = ClassUtil.findClasses(Function.class, "org.apache.tajo.engine.function");

    for (Class eachClass : functionClasses) {
      if(eachClass.isInterface() || Modifier.isAbstract(eachClass.getModifiers())) {
        continue;
      }
      Function function = null;
      try {
        function = (Function)eachClass.newInstance();
      } catch (Exception e) {
        LOG.warn(eachClass + " cannot instantiate Function class because of " + e.getMessage(), e);
        continue;
      }
      String functionName = function.getClass().getAnnotation(Description.class).functionName();
      String[] synonyms = function.getClass().getAnnotation(Description.class).synonyms();
      String description = function.getClass().getAnnotation(Description.class).description();
      String detail = function.getClass().getAnnotation(Description.class).detail();
      String example = function.getClass().getAnnotation(Description.class).example();
      TajoDataTypes.Type returnType = function.getClass().getAnnotation(Description.class).returnType();
      ParamTypes[] paramArray = function.getClass().getAnnotation(Description.class).paramTypes();

      String[] allFunctionNames = null;
      if(synonyms != null && synonyms.length > 0) {
        allFunctionNames = new String[1 + synonyms.length];
        allFunctionNames[0] = functionName;
        System.arraycopy(synonyms, 0, allFunctionNames, 1, synonyms.length);
      } else {
        allFunctionNames = new String[]{functionName};
      }

      for(String eachFunctionName: allFunctionNames) {
        for (ParamTypes params : paramArray) {
          ParamOptionTypes[] paramOptionArray;
          if(params.paramOptionTypes() == null ||
              params.paramOptionTypes().getClass().getAnnotation(ParamTypes.class) == null) {
            paramOptionArray = new ParamOptionTypes[0];
          } else {
            paramOptionArray = params.paramOptionTypes().getClass().getAnnotation(ParamTypes.class).paramOptionTypes();
          }

          TajoDataTypes.Type[] paramTypes = params.paramTypes();
          if (paramOptionArray.length > 0)
            paramTypes = params.paramTypes().clone();

          for (int i=0; i < paramOptionArray.length + 1; i++) {
            FunctionDesc functionDesc = new FunctionDesc(eachFunctionName,
                function.getClass(), function.getFunctionType(),
                CatalogUtil.newSimpleDataType(returnType),
                paramTypes.length == 0 ? CatalogUtil.newSimpleDataTypeArray() : CatalogUtil.newSimpleDataTypeArray(paramTypes));

            functionDesc.setDescription(description);
            functionDesc.setExample(example);
            functionDesc.setDetail(detail);
            sqlFuncs.add(functionDesc);

            if (i != paramOptionArray.length) {
              paramTypes = new TajoDataTypes.Type[paramTypes.length +
                  paramOptionArray[i].paramOptionTypes().length];
              System.arraycopy(params.paramTypes(), 0, paramTypes, 0, paramTypes.length);
              System.arraycopy(paramOptionArray[i].paramOptionTypes(), 0, paramTypes, paramTypes.length,
                  paramOptionArray[i].paramOptionTypes().length);
            }
          }
        }
      }
    }

    return sqlFuncs;
  }

  public static Collection<FunctionDesc> loadFunctions(TajoConf conf) throws IOException, AmbiguousFunctionException {
    List<FunctionDesc> functionList = new ArrayList<>(loadBuiltinFunctions().values());
    List<FunctionDesc> udfs = loadUserDefinedFunctions(conf);

    /* NOTE:
     * For built-in functions, it is not done to check duplicates.
     * There are two reasons.
     * Firstly, it could be an useless operation in most of cases because built-in functions are not changed frequently
     *   but checking will be done each startup.
     * Secondly, this logic checks duplicate excluding type, but there are already duplicates in built-in functions
     *   such as sum with/without 'distinct' feature. So to check duplicates in built-in functions, some other logic is needed.
     * It should be another issue.
     */
    // merge lists and return it.
    return mergeFunctionLists(functionList, udfs);
  }

  @SafeVarargs
  static Collection<FunctionDesc> mergeFunctionLists(List<FunctionDesc> ... functionLists)
      throws AmbiguousFunctionException {

    Map<Integer, FunctionDesc> funcMap = new HashMap<>();
    List<FunctionDesc> baseFuncList = functionLists[0];

    // Build a map with a first list
    for (FunctionDesc desc: baseFuncList) {
      funcMap.put(desc.hashCodeWithoutType(), desc);
    }

    // Check duplicates for other function lists(should be UDFs practically)
    for (int i=1; i<functionLists.length; i++) {
      for (FunctionDesc desc: functionLists[i]) {
        if (funcMap.containsKey(desc.hashCodeWithoutType())) {
          FunctionDesc storedDesc = funcMap.get(desc.hashCodeWithoutType());
          if (storedDesc.equalsSignature(desc)) {
            throw new AmbiguousFunctionException(String.format("%s", storedDesc.toString()));
          }
        }

        funcMap.put(desc.hashCodeWithoutType(), desc);
        baseFuncList.add(desc);
      }
    }

    return baseFuncList;
  }
}
