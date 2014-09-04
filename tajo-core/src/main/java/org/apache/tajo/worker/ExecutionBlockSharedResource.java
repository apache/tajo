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

package org.apache.tajo.worker;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.SessionVars;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.codegen.ExecutorPreCompiler;
import org.apache.tajo.engine.codegen.TajoClassLoader;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.planner.PlanningException;
import org.apache.tajo.engine.planner.logical.LogicalNode;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.storage.TupleComparator;
import org.apache.tajo.storage.TupleComparatorImpl;
import org.apache.tajo.util.Pair;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutionBlockSharedResource {
  private static Log LOG = LogFactory.getLog(ExecutionBlockSharedResource.class);
  private AtomicBoolean initializing = new AtomicBoolean(false);
  private volatile Boolean resourceInitSuccess = new Boolean(false);
  private CountDownLatch initializedResourceLatch = new CountDownLatch(1);

  // Query
  private QueryContext context;

  // Resources
  private TajoClassLoader classLoader;
  private ExecutorPreCompiler.CompilationContext compilationContext;
  private LogicalNode plan;
  private boolean codeGenEnabled = false;

  public void initialize(final QueryContext context, final String planJson) throws InterruptedException {

    if (!initializing.getAndSet(true)) {
      Thread thread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            ExecutionBlockSharedResource.this.context = context;
            initPlan(planJson);
            initCodeGeneration();
            resourceInitSuccess = true;
          } catch (Throwable t) {
            LOG.error(t);
            LOG.error(ExceptionUtils.getStackTrace(t));
          } finally {
            initializedResourceLatch.countDown();
          }
        }
      });
      thread.run();
      thread.join();

      if (!resourceInitSuccess) {
        throw new RuntimeException("Resource cannot be initialized");
      }
    }
  }

  private void initPlan(String planJson) {
    plan = CoreGsonHelper.fromJson(planJson, LogicalNode.class);
  }

  private void initCodeGeneration() throws PlanningException {
    if (context.getBool(SessionVars.CODEGEN)) {
      codeGenEnabled = true;
      classLoader = new TajoClassLoader();
      compilationContext = new ExecutorPreCompiler.CompilationContext(classLoader);
      ExecutorPreCompiler.compile(compilationContext, plan);
    }
  }

  public boolean awaitInitializedResource() throws InterruptedException {
    initializedResourceLatch.await();
    return resourceInitSuccess;
  }

  public LogicalNode getPlan() {
    return this.plan;
  }

  public EvalNode compileEval(Schema schema, EvalNode eval) {
    return compilationContext.getEvalCompiler().compile(schema, eval);
  }

  public EvalNode getCompiledEval(Schema schema, EvalNode eval) {
    if (codeGenEnabled) {

      Pair<Schema, EvalNode> key = new Pair<Schema, EvalNode>(schema, eval);
      if (compilationContext.getPrecompiedEvals().containsKey(key)) {
        return compilationContext.getPrecompiedEvals().get(key);
      } else {
        try {
          LOG.warn(eval.toString() + " does not exist. Compiling it immediately.");
          return compileEval(schema, eval);
        } catch (Throwable t) {
          LOG.warn(t);
          return eval;
        }
      }
    } else {
      throw new IllegalStateException("CODEGEN is disabled");
    }
  }

  public TupleComparator compileComparator(Schema schema, TupleComparatorImpl comp) {
    return compilationContext.getComparatorCompiler().compile(comp, false);
  }

  public TupleComparator getCompiledComparator(Schema schema, TupleComparatorImpl comp) {
    if (codeGenEnabled) {
      Pair<Schema, TupleComparatorImpl> key = new Pair<Schema, TupleComparatorImpl>(schema, comp);
      if (compilationContext.getPrecompiedComparators().containsKey(key)) {
        return compilationContext.getPrecompiedComparators().get(key);
      } else {
        try {
          LOG.warn(comp + " does not exist. Compiling it immediately");
          return compileComparator(schema, comp);
        } catch (Throwable t) {
          LOG.warn(t);
          return comp;
        }
      }
    } else {
      throw new IllegalStateException("CODEGEN is disabled");
    }
  }

  public TajoClassLoader getClassLoader() {
    return classLoader;
  }

  public void release() {
    compilationContext = null;

    if (classLoader != null) {
      try {
        classLoader.clean();
      } catch (Throwable throwable) {
        throwable.printStackTrace();
      }
      classLoader = null;
    }
  }
}
