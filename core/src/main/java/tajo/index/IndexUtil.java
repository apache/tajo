package tajo.index;

import com.google.gson.Gson;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.engine.exec.eval.ConstEval;
import tajo.engine.exec.eval.EvalNode;
import tajo.engine.exec.eval.EvalNode.Type;
import tajo.engine.exec.eval.EvalNodeVisitor;
import tajo.engine.exec.eval.FieldEval;
import tajo.engine.ipc.protocolrecords.Fragment;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.SortSpec;
import tajo.engine.planner.logical.IndexScanNode;
import tajo.engine.planner.logical.ScanNode;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

public class IndexUtil {
  public static String getIndexNameOfFrag(Fragment fragment, SortSpec[] keys) {
    StringBuilder builder = new StringBuilder(); 
    builder.append(fragment.getPath().getName() + "_");
    builder.append(fragment.getStartOffset() + "_" + fragment.getLength() + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getSortKey().getColumnName()+"_");
    }
    builder.append("_index");
    return builder.toString();
       
  }
  
  public static String getIndexName (String indexName , SortSpec[] keys) {
    StringBuilder builder = new StringBuilder();
    builder.append(indexName + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getSortKey().getColumnName() + "_");
    }
    return builder.toString();
  }
  
  public static IndexScanNode indexEval( ScanNode scanNode, 
      Iterator<Entry<String, String>> iter ) {
   
    EvalNode qual = scanNode.getQual();
    Gson gson = GsonCreator.getInstance();
    
    FieldAndValueFinder nodeFinder = new FieldAndValueFinder();
    qual.preOrder(nodeFinder);
    LinkedList<EvalNode> nodeList = nodeFinder.getNodeList();
    
    int maxSize = Integer.MIN_VALUE;
    SortSpec[] maxIndex = null;
    
    String json;
    while(iter.hasNext()) {
      Entry<String , String> entry = iter.next();
      json = entry.getValue();
      SortSpec[] sortKey = gson.fromJson(json, SortSpec[].class);
      if(sortKey.length > nodeList.size()) {
        /* If the number of the sort key is greater than where condition, 
         * this index cannot be used
         * */
        continue; 
      } else {
        boolean[] equal = new boolean[sortKey.length];
        for(int i = 0 ; i < sortKey.length ; i ++) {
          for(int j = 0 ; j < nodeList.size() ; j ++) {
            Column col = ((FieldEval)(nodeList.get(j).getLeftExpr())).getColumnRef();
            if(col.equals(sortKey[i].getSortKey())) {
              equal[i] = true;
            }
          }
        }
        boolean chk = true;
        for(int i = 0 ; i < equal.length ; i ++) {
          chk = chk && equal[i];
        }
        if(chk) {
          if(maxSize < sortKey.length) {
            maxSize = sortKey.length;
            maxIndex = sortKey;
          }
        }
      }
    }
    if(maxIndex == null) {
      return null;
    } else {
      Schema keySchema = new Schema();
      for(int i = 0 ; i < maxIndex.length ; i ++ ) {
        keySchema.addColumn(maxIndex[i].getSortKey());
      }
      Datum[] datum = new Datum[nodeList.size()];
      for(int i = 0 ; i < nodeList.size() ; i ++ ) {
        datum[i] = ((ConstEval)(nodeList.get(i).getRightExpr())).getValue();
      }
      
      return new IndexScanNode(scanNode, keySchema , datum , maxIndex);
    }

  }
  
  
  private static class FieldAndValueFinder implements EvalNodeVisitor {
    private LinkedList<EvalNode> nodeList = new LinkedList<EvalNode>();
    
    public LinkedList<EvalNode> getNodeList () {
      return this.nodeList;
    }
    
    @Override
    public void visit(EvalNode node) {
      switch(node.getType()) {
      case AND:
        break;
      case EQUAL:
        if( node.getLeftExpr().getType() == Type.FIELD 
          && node.getRightExpr().getType() == Type.CONST ) {
          nodeList.add(node);
        }
        break;
      case IS:
        if( node.getLeftExpr().getType() == Type.FIELD 
          && node.getRightExpr().getType() == Type.CONST) {
          nodeList.add(node);
        }
      }
    }
  }
}
