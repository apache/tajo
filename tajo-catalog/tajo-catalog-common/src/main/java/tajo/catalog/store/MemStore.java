/**
 * 
 */
package tajo.catalog.store;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import tajo.catalog.FunctionDesc;
import tajo.catalog.TableDesc;
import tajo.catalog.proto.CatalogProtos.IndexDescProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Hyunsik Choi
 */
public class MemStore implements CatalogStore {
  private final Map<String,TableDesc> tables
    = Maps.newHashMap();
  private final Map<String, FunctionDesc> functions
    = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexes
    = Maps.newHashMap();
  private final Map<String, IndexDescProto> indexesByColumn
  = Maps.newHashMap();
  
  public MemStore(Configuration conf) {
  }

  /* (non-Javadoc)
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    tables.clear();
    functions.clear();
    indexes.clear();
  }

  /* (non-Javadoc)
   * @see CatalogStore#addTable(TableDesc)
   */
  @Override
  public void addTable(TableDesc desc) throws IOException {
    synchronized(tables) {
      tables.put(desc.getId(), desc);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#existTable(java.lang.String)
   */
  @Override
  public boolean existTable(String name) throws IOException {
    synchronized(tables) {
      return tables.containsKey(name);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#deleteTable(java.lang.String)
   */
  @Override
  public void deleteTable(String name) throws IOException {
    synchronized(tables) {
      tables.remove(name);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getTable(java.lang.String)
   */
  @Override
  public TableDesc getTable(String name) throws IOException {
    return tables.get(name);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllTableNames()
   */
  @Override
  public List<String> getAllTableNames() throws IOException {
    return new ArrayList<String>(tables.keySet());
  }

  /* (non-Javadoc)
   * @see CatalogStore#addIndex(nta.catalog.proto.CatalogProtos.IndexDescProto)
   */
  @Override
  public void addIndex(IndexDescProto proto) throws IOException {
    synchronized(indexes) {
      indexes.put(proto.getName(), proto);
      indexesByColumn.put(proto.getTableId() + "." 
          + proto.getColumn().getColumnName(), proto);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#delIndex(java.lang.String)
   */
  @Override
  public void delIndex(String indexName) throws IOException {
    synchronized(indexes) {
      indexes.remove(indexName);
    }
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String indexName) throws IOException {
    return indexes.get(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndex(java.lang.String, java.lang.String)
   */
  @Override
  public IndexDescProto getIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.get(tableName+"."+columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String)
   */
  @Override
  public boolean existIndex(String indexName) throws IOException {
    return indexes.containsKey(indexName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#existIndex(java.lang.String, java.lang.String)
   */
  @Override
  public boolean existIndex(String tableName, String columnName)
      throws IOException {
    return indexesByColumn.containsKey(tableName + "." + columnName);
  }

  /* (non-Javadoc)
   * @see CatalogStore#getIndexes(java.lang.String)
   */
  @Override
  public IndexDescProto[] getIndexes(String tableName) throws IOException {
    List<IndexDescProto> protos = new ArrayList<IndexDescProto>();
    for (IndexDescProto proto : indexesByColumn.values()) {
      if (proto.getTableId().equals(tableName)) {
        protos.add(proto);
      }
    }
    return protos.toArray(new IndexDescProto[protos.size()]);
  }

  /* (non-Javadoc)
   * @see CatalogStore#addFunction(FunctionDesc)
   */
  @Override
  public void addFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#deleteFunction(FunctionDesc)
   */
  @Override
  public void deleteFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#existFunction(FunctionDesc)
   */
  @Override
  public void existFunction(FunctionDesc func) throws IOException {
    // to be implemented
  }

  /* (non-Javadoc)
   * @see CatalogStore#getAllFunctionNames()
   */
  @Override
  public List<String> getAllFunctionNames() throws IOException {
    // to be implemented
    return null;
  }

}
