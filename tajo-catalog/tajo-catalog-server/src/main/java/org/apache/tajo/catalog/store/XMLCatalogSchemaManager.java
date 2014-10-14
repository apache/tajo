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

package org.apache.tajo.catalog.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.exception.CatalogException;
import org.apache.tajo.catalog.store.object.BaseSchema;
import org.apache.tajo.catalog.store.object.DatabaseObject;
import org.apache.tajo.catalog.store.object.DatabaseObjectType;
import org.apache.tajo.catalog.store.object.ExistQuery;
import org.apache.tajo.catalog.store.object.SchemaPatch;
import org.apache.tajo.catalog.store.object.StoreObject;

public class XMLCatalogSchemaManager {
  protected final Log LOG = LogFactory.getLog(getClass());
  private String schemaPath;
  private final Unmarshaller unmarshaller;
  private StoreObject catalogStore;
  
  public XMLCatalogSchemaManager(String schemaPath) throws CatalogException {
    this.schemaPath = schemaPath;
    try {
      JAXBContext context = JAXBContext.newInstance(StoreObject.class);
      unmarshaller = context.createUnmarshaller();
      
      loadFromXmlFiles();
    } catch (Exception e) {
      throw new CatalogException(e.getMessage(), e);
    }
  }

  public void dropBaseSchema(Connection conn) throws CatalogException {
    if (!isLoaded()) {
      throw new CatalogException("Schema files are not loaded yet.");
    }
    
    List<DatabaseObject> failedObjects = new ArrayList<DatabaseObject>();
    Statement stmt = null;
    
    try {
      stmt = conn.createStatement();
    } catch (SQLException e) {
      throw new CatalogException(e.getMessage(), e);
    }
    
    for (DatabaseObject object: catalogStore.getSchema().getObjects()) {
      try {
        if (DatabaseObjectType.TABLE == object.getType() ||
            DatabaseObjectType.SEQUENCE == object.getType() ||
            DatabaseObjectType.VIEW == object.getType()) {
          stmt.executeUpdate("drop " + object.getType().toString() + " " + object.getName());
        }
      } catch (SQLException e) {
        failedObjects.add(object);
      }
    }
    
    if (failedObjects.size() > 0) {
      StringBuffer errorMessage = new StringBuffer(64);
      errorMessage.append("Failed to drop database objects ");
      
      for (int idx = 0; idx < failedObjects.size(); idx++) {
        DatabaseObject object = failedObjects.get(idx);
        errorMessage.append(object.getType().toString()).append(" ")
          .append(object.getName());
        if ((idx+1) < failedObjects.size()) {
          errorMessage.append(',');
        }
      }
      
      LOG.warn(errorMessage.toString());
    }
  }
  
  protected PreparedStatement getExistQuery(Connection conn, DatabaseObjectType type) 
      throws SQLException {
    PreparedStatement pstmt = null;
    
    for (ExistQuery existQuery: catalogStore.getQueries()) {
      if (type.equals(existQuery.getType())) {
        pstmt = conn.prepareStatement(existQuery.getSql());
        break;
      }
    }
    
    return pstmt;
  }
  
  protected boolean checkExistance(Connection conn, DatabaseObjectType type, String... params) 
      throws SQLException {
    boolean result = false;
    DatabaseMetaData metadata = null;
    PreparedStatement pstmt = null;
    BaseSchema baseSchema = catalogStore.getSchema();
    
    switch(type) {
    case DATA:
      metadata = conn.getMetaData();
      ResultSet data = metadata.getUDTs(null, baseSchema.getSchemaName() != null && !baseSchema.getSchemaName().isEmpty()?
          baseSchema.getSchemaName().toUpperCase():null, 
          params[0].toUpperCase(), null);
      result = data.next();
      data.close();
      break;
    case FUNCTION:
      metadata = conn.getMetaData();
      ResultSet functions = metadata.getFunctions(null, baseSchema.getSchemaName() != null && !baseSchema.getSchemaName().isEmpty()?
          baseSchema.getSchemaName().toUpperCase():null,
          params[0].toUpperCase());
      result = functions.next();
      functions.close();
      break;
    case INDEX:
      if (params.length != 2) {
        throw new IllegalArgumentException("Finding index object is needed two strings, table name and index name");
      }
      
      metadata = conn.getMetaData();
      ResultSet indexes = metadata.getIndexInfo(null, baseSchema.getSchemaName() != null && !baseSchema.getSchemaName().isEmpty()?
          baseSchema.getSchemaName().toUpperCase():null, 
          params[0].toUpperCase(), false, true);
      while (indexes.next()) {
        if (indexes.getString("INDEX_NAME").equals(params[1].toUpperCase())) {
          result = true;
          break;
        }
      }
      indexes.close();
      break;
    case TABLE:
      metadata = conn.getMetaData();
      ResultSet tables = metadata.getTables(null, baseSchema.getSchemaName() != null && !baseSchema.getSchemaName().isEmpty()?
          baseSchema.getSchemaName().toUpperCase():null, 
          params[0].toUpperCase(), new String[] {"TABLE"});
      result = tables.next();
      tables.close();
      break;
    case DOMAIN:
    case OPERATOR:
    case RULE:
    case SEQUENCE:
    case TRIGGER:
    case VIEW:
      pstmt = getExistQuery(conn, type);
      
      if (pstmt == null) {
        throw new CatalogException("Finding " + type
            + " type of database object is not supported on this database system.");
      }
      
      int paramIdx = 1;
      
      if (baseSchema.getSchemaName() != null && !baseSchema.getSchemaName().isEmpty()) {
        pstmt.setString(paramIdx++, baseSchema.getSchemaName().toUpperCase());
      }
      
      for (; paramIdx <= pstmt.getParameterMetaData().getParameterCount(); paramIdx++) {
        pstmt.setString(paramIdx, params[paramIdx-1]);
      }
      
      ResultSet rs = pstmt.executeQuery();
      while (rs.next()) {
        if (rs.getString(1).equals(params[params.length-1].toUpperCase())) {
          result = true;
          break;
        }
      }
      rs.close();
      break;
    }
    
    return result;
  }

  public void createBaseSchema(Connection conn) throws CatalogException {
    Statement stmt;
    
    if (!isLoaded()) {
      throw new CatalogException("Database schema files are not loaded.");
    }
    
    try {
      stmt = conn.createStatement();
    } catch (SQLException e) {
      throw new CatalogException(e.getMessage(), e);
    }
    
    for (DatabaseObject object: catalogStore.getSchema().getObjects()) {
      try {
        String[] params;
        if (DatabaseObjectType.INDEX == object.getType()) {
          params = new String[2];
          params[0] = object.getDependsOn();
          params[1] = object.getName();
        }
        else {
          params = new String[1];
          params[0] = object.getName();
        }
        
        if (checkExistance(conn, object.getType(), params)) {
          LOG.info("Skip to create " + object.getName() + " databse object. Already exists.");
        }
        else {
          stmt.executeUpdate(object.getSql());
          LOG.info(object.getName() + " " + object.getType() + " is created.");
        }
      } catch (SQLException e) {
        throw new CatalogException(e.getMessage(), e);
      }
    }
  }
  
  public void upgradeBaseSchema(Connection conn, int currentVersion) {
    if (!isLoaded()) {
      throw new CatalogException("Database schema files are not loaded.");
    }
    
    final List<SchemaPatch> candidatePatches = new ArrayList<SchemaPatch>();
    Statement stmt;
    
    for (SchemaPatch patch: this.catalogStore.getPatches()) {
      if (currentVersion >= patch.getPriorVersion()) {
        candidatePatches.add(patch);
      }
    }
    
    Collections.sort(candidatePatches);
    try {
      stmt = conn.createStatement();
    } catch (SQLException e) {
      throw new CatalogException(e.getMessage(), e);
    }
    
    for (SchemaPatch patch: candidatePatches) {
      for (DatabaseObject object: patch.getObjects()) {
        try {
          stmt.executeUpdate(object.getSql());
          LOG.info(object.getName() + " " + object.getType() + " was created or altered.");
        } catch (SQLException e) {
          throw new CatalogException(e.getMessage(), e);
        }
      }
    }
  }

  public boolean isInitialized(Connection conn) throws CatalogException, SQLException {
    if (!isLoaded()) {
      throw new CatalogException("Database schema files are not loaded.");
    }
    boolean result = true;
    
    for (DatabaseObject object: catalogStore.getSchema().getObjects()) {
      if (DatabaseObjectType.INDEX == object.getType()) {
        result &= checkExistance(conn, object.getType(), object.getDependsOn(), object.getName());
      }
      else {
        result &= checkExistance(conn, object.getType(), object.getName());
      }
      
      if (!result) {
        break;
      }
    }
    return result;
  }
  
  protected String[] listFileResources(URL dirURL, String schemaPath, FilenameFilter filter) 
      throws URISyntaxException, IOException {
    String[] files = new String[0];
    String[] tempFiles;
    List<String> filesList = new ArrayList<String>();
    File dirFile = new File(dirURL.toURI());
    
    tempFiles = dirFile.list(filter);
    files = new String[tempFiles.length];
    
    for (String fileName: tempFiles) {
      filesList.add(schemaPath+"/"+fileName);
    }
    
    files = filesList.toArray(files);
    return files;
  }
  
  protected String[] listJarResources(URL dirURL, FilenameFilter filter) 
      throws IOException, URISyntaxException {
    String[] files = new String[0];
    String spec = dirURL.getFile();
    int seperator = spec.indexOf("!/");
    
    if (seperator == -1) {
      return files;
    }

    URL jarFileURL = new URL(spec.substring(0, seperator));
    JarFile jarFile = new JarFile(jarFileURL.toURI().getPath());
    Set<String> filesSet = new HashSet<String>();
    
    try {
      Enumeration<JarEntry> entries = jarFile.entries();
      
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        
        if (entry.isDirectory()) {
          continue;
        }
        
        String entryName = entry.getName();
        
        if (entryName.indexOf(schemaPath) > -1 &&
            filter.accept(null, entryName)) {
          filesSet.add(entryName);
        }
      }
    } finally {
      jarFile.close();
    }
    
    if (!filesSet.isEmpty()) {
      files = new String[filesSet.size()];
      files = filesSet.toArray(files);
    }
    
    return files;
  }
  
  protected String[] listResources() throws IOException, URISyntaxException {
    String[] files = new String[0];
    URL dirURL = ClassLoader.getSystemResource(schemaPath);
    FilenameFilter fileFilter = new FilenameFilter() {
      
      @Override
      public boolean accept(File dir, String name) {
        return ((name.lastIndexOf('.') > -1) && 
            (".xml".equalsIgnoreCase(name.substring(name.lastIndexOf('.')))));
      }
    };
    
    if (dirURL == null) {
      throw new FileNotFoundException(schemaPath);
    }
    
    if (dirURL.getProtocol().equals("file")) {
      files = listFileResources(dirURL, schemaPath, fileFilter);
    }
    
    if (dirURL.getProtocol().equals("jar")) {
      files = listJarResources(dirURL, fileFilter);
    }
    
    return files;
  }
  
  protected void mergeXmlSchemas(final List<StoreObject> storeObjects) throws CatalogException {
    if (storeObjects.size() <= 0) {
      throw new CatalogException("Unable to find a schema file.");
    }
    
    this.catalogStore = new StoreObjectsMerger(storeObjects).merge();
  }
  
  protected void loadFromXmlFiles() throws IOException, XMLStreamException, URISyntaxException {
    XMLInputFactory xmlIf = XMLInputFactory.newInstance();
    final List<StoreObject> storeObjects = new ArrayList<StoreObject>();
    
    xmlIf.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    
    for (String resname: listResources()) {
      URL filePath = ClassLoader.getSystemResource(resname);
      
      if (filePath == null) {
        throw new FileNotFoundException(resname);
      }
      
      loadFromXmlFile(xmlIf, filePath, storeObjects);
    }
    
    mergeXmlSchemas(storeObjects);
  }
  
  protected void loadFromXmlFile(XMLInputFactory xmlIf, URL filePath, List<StoreObject> storeObjects) 
      throws IOException, XMLStreamException {
    XMLStreamReader xmlReader;

    xmlReader = xmlIf.createXMLStreamReader(filePath.openStream());
    
    try {
      while (xmlReader.hasNext()) {
        if (xmlReader.next() == XMLStreamConstants.START_ELEMENT &&
            "store".equals(xmlReader.getLocalName())) {
          StoreObject catalogStore = loadCatalogStore(xmlReader);
          if (catalogStore != null) {
            storeObjects.add(catalogStore);
          }
        }
      }
    } finally {
      try {
        xmlReader.close();
      } catch (XMLStreamException ignored) { }
    }
  }
  
  protected StoreObject loadCatalogStore(XMLStreamReader xmlReader) throws XMLStreamException {
    try {
      JAXBElement<StoreObject> elem = unmarshaller.unmarshal(xmlReader, StoreObject.class);
      return elem.getValue();
    } catch (JAXBException e) {
      throw new XMLStreamException(e.getMessage(), xmlReader.getLocation(), e);
    }
  }
  
  protected StoreObject getCatalogStore() {
    return catalogStore;
  }
  
  public boolean isLoaded() {
    return catalogStore!=null;
  }
  
  private class StoreObjectsMerger {
    
    private final List<StoreObject> storeObjects = new ArrayList<StoreObject>();
    private final StoreObject targetStore = new StoreObject();
    
    public StoreObjectsMerger(List<StoreObject> schemaObjects) {
      this.storeObjects.addAll(schemaObjects);
    }
    
    protected void copySchemaInfo(StoreObject sourceStore) {
      if (sourceStore.getSchema().getSchemaName() != null && 
          !sourceStore.getSchema().getSchemaName().isEmpty()) {
        if (targetStore.getSchema().getSchemaName() != null && 
            !targetStore.getSchema().getSchemaName().isEmpty() &&
            !targetStore.getSchema().getSchemaName().equalsIgnoreCase(
                sourceStore.getSchema().getSchemaName())) {
          throw new CatalogException("different schema names are specified. One is " + 
            sourceStore.getSchema().getSchemaName() + " and other is " + 
              targetStore.getSchema().getSchemaName());
        }
        
        if (targetStore.getSchema().getSchemaName() == null || 
            targetStore.getSchema().getSchemaName().isEmpty()) {
          targetStore.getSchema().setSchemaName(sourceStore.getSchema().getSchemaName());
        }
      }
      
      if (sourceStore.getSchema().getVersion() > -1 && 
          targetStore.getSchema().getVersion() < sourceStore.getSchema().getVersion()) {
        targetStore.getSchema().setVersion(sourceStore.getSchema().getVersion());
      }
    }
    
    protected List<DatabaseObject> mergeDatabaseObjects(List<DatabaseObject> objects) {
      final List<DatabaseObject> orderedObjects = new ArrayList<DatabaseObject>(objects.size());
      final List<DatabaseObject> unorderedObjects = new ArrayList<DatabaseObject>(objects.size());
      final List<DatabaseObject> mergedObjects = new ArrayList<DatabaseObject>(objects.size());
      
      for (DatabaseObject object: objects) {
        if (object.getOrder() > -1) {
          int objIdx = object.getOrder();
          
          if (objIdx < orderedObjects.size() && orderedObjects.get(objIdx) != null) {
            throw new CatalogException("This catalog configuration contains duplicated order of DatabaseObject");
          }
          
          orderedObjects.add(objIdx, object);
        }
        else {
          unorderedObjects.add(object);
        }
      }
      
      for (DatabaseObject object: orderedObjects) {
        if (object != null) {
          mergedObjects.add(object);
        }
      }
      
      for (DatabaseObject object: unorderedObjects) {
        if (object != null) {
          mergedObjects.add(object);
        }
      }
      
      return mergedObjects;
    }
    
    protected void validatePatch(List<SchemaPatch> patches, SchemaPatch testPatch) {
      if (testPatch.getPriorVersion() > testPatch.getNextVersion()) {
        throw new CatalogException("Prior version cannot proceed to next version of patch.");
      }
      
      for (SchemaPatch patch: patches) {
        if (testPatch.equals(patch)) {
          continue;
        }
        
        if (testPatch.getPriorVersion() == patch.getPriorVersion()) {
          LOG.warn("It has the same prior version (" + testPatch.getPriorVersion() + ") of patch.");
          
          if (testPatch.getNextVersion() == patch.getNextVersion()) {
            throw new CatalogException("Duplicate versions of patch found. It will terminate Catalog Store. ");
          }
        }
        
        if (testPatch.getNextVersion() == patch.getNextVersion()) {
          LOG.warn("It has the same next version (" + testPatch.getPriorVersion() + ") of patch.");
        }
      }
    }
    
    protected void mergePatches(List<SchemaPatch> patches) {
      final List<DatabaseObject> objects = new ArrayList<DatabaseObject>();
      
      Collections.sort(patches);
      
      for (SchemaPatch patch: patches) {
        validatePatch(patches, patch);
        
        objects.clear();
        List<DatabaseObject> tempObjects = new ArrayList<DatabaseObject>();
        tempObjects.addAll(patch.getObjects());
        patch.clearObjects();
        patch.addObjects(mergeDatabaseObjects(tempObjects));        
        
        targetStore.addPatch(patch);
      }
    }
    
    protected void validateExistQuery(List<ExistQuery> queries, ExistQuery testQuery) {
      int occurredCount = 0;
      
      for (ExistQuery query: queries) {
        if (query.getType() == testQuery.getType()) {
          occurredCount++;
        }
      }
      
      if (occurredCount > 1) {
        throw new CatalogException("Duplicate Query type (" + testQuery.getType() + ") has found.");
      }
    }
    
    protected void mergeExistQueries(List<ExistQuery> queries) {
      for (ExistQuery query: queries) {
        validateExistQuery(queries, query);
        
        targetStore.addQuery(query);
      }
    }
    
    public StoreObject merge() {
      boolean alreadySetDatabaseObject = false;
      
      // first pass
      for (StoreObject store : this.storeObjects) {
        copySchemaInfo(store);
      }
      
      // second pass
      for (StoreObject store: this.storeObjects) {
        if (store.getSchema().getVersion() == targetStore.getSchema().getVersion() && 
            !alreadySetDatabaseObject) {
          BaseSchema targetSchema = targetStore.getSchema();
          targetSchema.clearObjects();
          targetSchema.addObjects(mergeDatabaseObjects(store.getSchema().getObjects()));
          
          alreadySetDatabaseObject = true;
        }
        
        mergePatches(store.getPatches());
        mergeExistQueries(store.getQueries());
      }
      
      return this.targetStore;
    }
  }

}
