/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner;

import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.engine.eval.EvalContext;
import tajo.engine.eval.EvalNode;
import tajo.engine.parser.QueryBlock.Target;
import tajo.storage.Tuple;

/**
 * @author Hyunsik Choi
 */
public class Projector {
  private final Schema inSchema;
  private final Schema outSchema;

  // for projection
  private final int targetNum;
  private final int [] inMap;
  private final int [] outMap;
  private int [] evalOutMap; // target list?
  private EvalNode[] evals;
  private Tuple prevTuple;

  public Projector(Schema inSchema, Schema outSchema, Target [] targets) {
    this.inSchema = inSchema;
    this.outSchema = outSchema;

    this.targetNum = targets != null ? targets.length : 0;

    inMap = new int[outSchema.getColumnNum() - targetNum];
    outMap = new int[outSchema.getColumnNum() - targetNum];
    int mapId = 0;
    Column col;

    if (targetNum > 0) {
      evalOutMap = new int[targetNum];
      evals = new EvalNode[targetNum];
      for (int i = 0; i < targetNum; i++) {
        // TODO - is it always  correct?
        if (targets[i].hasAlias()) {
          evalOutMap[i] = outSchema.getColumnId(targets[i].getAlias());
        } else {
          evalOutMap[i] = outSchema.getColumnId(targets[i].getEvalTree().getName());
        }
        evals[i] = targets[i].getEvalTree();
      }

      outer:
      for (int targetId = 0; targetId < outSchema.getColumnNum(); targetId ++) {
        for (int j = 0; j < evalOutMap.length; j++) {
          if (evalOutMap[j] == targetId)
            continue outer;
        }

        col = inSchema.getColumn(outSchema.getColumn(targetId).getQualifiedName());
        outMap[mapId] = targetId;
        inMap[mapId] = inSchema.getColumnId(col.getQualifiedName());
        mapId++;
      }
    } else {
      for (int targetId = 0; targetId < outSchema.getColumnNum(); targetId ++) {
        col = inSchema.getColumn(outSchema.getColumn(targetId).getQualifiedName());
        outMap[mapId] = targetId;
        inMap[mapId] = inSchema.getColumnId(col.getQualifiedName());
        mapId++;
      }
    }
  }

  public void eval(EvalContext[] evalContexts, Tuple in) {
    this.prevTuple = in;
    if (targetNum > 0) {
      for (int i = 0; i < evals.length; i++) {
        evals[i].eval(evalContexts[i], inSchema, in);
      }
    }
  }

  public void terminate(EvalContext [] evalContexts, Tuple out) {
    for (int i = 0; i < inMap.length; i++) {
      out.put(outMap[i], prevTuple.get(inMap[i]));
    }
    if (targetNum > 0) {
      for (int i = 0; i < evals.length; i++) {
        out.put(evalOutMap[i], evals[i].terminate(evalContexts[i]));
      }
    }
  }

  public EvalContext [] renew() {
    EvalContext [] evalContexts = new EvalContext[targetNum];
    for (int i = 0; i < targetNum; i++) {
      evalContexts[i] = evals[i].newContext();
    }

    return evalContexts;
  }
}
