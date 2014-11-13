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

package org.apache.tajo.catalog.store.object;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;

@XmlAccessorType(XmlAccessType.FIELD)
public class StoreObject {

  @XmlElement(name="base",namespace="http://tajo.apache.org/catalogstore")
  private BaseSchema schema = new BaseSchema();
  @XmlElementWrapper(name="existQueries",namespace="http://tajo.apache.org/catalogstore")
  @XmlElement(name="existQuery",namespace="http://tajo.apache.org/catalogstore")
  private final List<SQLObject> existQueries = new ArrayList<SQLObject>();
  @XmlElementWrapper(name="dropStatements",namespace="http://tajo.apache.org/catalogstore")
  @XmlElement(name="dropStatement",namespace="http://tajo.apache.org/catalogstore")
  private final List<SQLObject> dropStatements = new ArrayList<SQLObject>();
  @XmlElementWrapper(name="patches",namespace="http://tajo.apache.org/catalogstore")
  @XmlElement(name="patch",namespace="http://tajo.apache.org/catalogstore")
  private final List<SchemaPatch> patches = new ArrayList<SchemaPatch>();
  
  public BaseSchema getSchema() {
    return schema;
  }
  public void setSchema(BaseSchema schema) {
    this.schema = schema;
  }
  public List<SQLObject> getExistQueries() {
    return existQueries;
  }
  public void addExistQuery(SQLObject query) {
    this.existQueries.add(query);
  }
  public void addExistQueries(Collection<SQLObject> queries) {
    this.existQueries.addAll(queries);
  }
  public void clearExistQueries() {
    this.existQueries.clear();
  }
  public List<SQLObject> getDropStatements() {
    return dropStatements;
  }
  public void addDropStatement(SQLObject query) {
    this.dropStatements.add(query);
  }
  public void addDropStatements(Collection<SQLObject> queries) {
    this.dropStatements.addAll(queries);
  }
  public void clearDropStatements() {
    this.dropStatements.clear();
  }
  public List<SchemaPatch> getPatches() {
    return patches;
  }
  public void addPatch(SchemaPatch patch) {
    this.patches.add(patch);
  }
  public void addPatches(Collection<SchemaPatch> patches) {
    this.patches.addAll(patches);
  }
  public void clearPatches() {
    this.patches.clear();
  }
}
