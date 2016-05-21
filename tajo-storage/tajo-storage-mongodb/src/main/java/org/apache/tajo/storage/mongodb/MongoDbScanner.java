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
package org.apache.tajo.storage.mongodb;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;

import java.io.IOException;
public class MongoDbScanner  implements Scanner {
    @Override
    public void init() throws IOException {

    }

    @Override
    public Tuple next() throws IOException {
        return null;
    }

    @Override
    public void reset() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void pushOperators(LogicalNode planPart) {

    }

    @Override
    public boolean isProjectable() {
        return false;
    }

    @Override
    public void setTarget(Column[] targets) {

    }

    @Override
    public boolean isSelectable() {
        return false;
    }

    @Override
    public void setFilter(EvalNode filter) {

    }

    @Override
    public void setLimit(long num) {

    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public TableStats getInputStats() {
        return null;
    }

    @Override
    public Schema getSchema() {
        return null;
    }
}
