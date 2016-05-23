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
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.SessionVars;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.engine.codegen.ExecutorPreCompiler;
import org.apache.tajo.engine.codegen.TajoClassLoader;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.engine.query.QueryContext;
import org.apache.tajo.engine.utils.CacheHolder;
import org.apache.tajo.engine.utils.TableCache;
import org.apache.tajo.engine.utils.TableCacheKey;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.pullserver.TajoPullServerService;
import org.apache.tajo.util.Pair;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutionBlockSharedResource {
  private static Log LOG = LogFactory.getLog(ExecutionBlockSharedResource.class);
  private AtomicBoolean initializing = new AtomicBoolean(false);
  private volatile Boolean resourceInitSuccess = Boolean.valueOf(false);
  private final Object lock = new Object();

  // Query
  private QueryContext context;

  // Resources
  private TajoClassLoader classLoader;
  private ExecutorPreCompiler.CompilationContext compilationContext;
  private LogicalNode plan;
  private boolean codeGenEnabled = false;
  private final TajoPullServerService pullServerService;

  public ExecutionBlockSharedResource() {
    this(null);
  }

  public ExecutionBlockSharedResource(@Nullable TajoPullServerService pullServerService) {
    this.pullServerService = pullServerService;
  }

  public void initialize(final QueryContext context, final String planJson) {

    if (!initializing.getAndSet(true)) {
      try {
        ExecutionBlockSharedResource.this.context = context;
        initCodeGeneration();
        resourceInitSuccess = true;
      } catch (Throwable t) {
        LOG.error(t);
        LOG.error(ExceptionUtils.getStackTrace(t));
      }

      if (!resourceInitSuccess) {
        throw new RuntimeException("Resource cannot be initialized");
      }
    }
  }

  private void initCodeGeneration() throws TajoException {
    if (context.getBool(SessionVars.CODEGEN)) {
      codeGenEnabled = true;
      classLoader = new TajoClassLoader();
      compilationContext = new ExecutorPreCompiler.CompilationContext(classLoader);
      ExecutorPreCompiler.compile(compilationContext, plan);
    }
  }

  public LogicalNode getPlan() {
    return this.plan;
  }

  public EvalNode compileEval(Schema schema, EvalNode eval) {
    return compilationContext.getCompiler().compile(schema, eval);
  }

  public EvalNode getPreCompiledEval(Schema schema, EvalNode eval) {
    if (codeGenEnabled) {

      Pair<Schema, EvalNode> key = new Pair<>(schema, eval);
      if (compilationContext.getPrecompiedEvals().containsKey(key)) {
        return compilationContext.getPrecompiedEvals().get(key);
      } else {
        try {
          LOG.warn(eval.toString() + " does not exists. Immediately compile it: " + eval);
          return compileEval(schema, eval);
        } catch (Throwable t) {
          LOG.warn(t, t);
          return eval;
        }
      }
    } else {
      throw new IllegalStateException("CodeGen is disabled");
    }
  }

  /* This is guarantee a lock for a ExecutionBlock */
  public synchronized Object getLock() {
    return lock;
  }

  public boolean hasBroadcastCache(TableCacheKey key) {
    return TableCache.getInstance().hasCache(key);
  }

  public <T extends Object> CacheHolder<T> getBroadcastCache(TableCacheKey key) {
    return (CacheHolder<T>) TableCache.getInstance().getCache(key);
  }

  public void addBroadcastCache(TableCacheKey cacheKey,  CacheHolder<?> cacheData) {
    TableCache.getInstance().addCache(cacheKey, cacheData);
  }

  public void releaseBroadcastCache(ExecutionBlockId id) {
    TableCache.getInstance().releaseCache(id);
  }

  public Optional<TajoPullServerService> getPullServerService() {
    return pullServerService == null ? Optional.empty() : Optional.of(pullServerService);
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
