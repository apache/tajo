package nta.engine;

import nta.catalog.TableDesc;
import nta.engine.parser.QueryBlock.Target;

/**
 * 이 클래스는 주어진 질의가 실행 중인 동안 질의에 대한 정보를 유지한다.
 * 
 * @author Hyunsik Choi
 */
public interface Context {  
  TableDesc getInputTable(String id);

  CatalogReader getCatalog();
  
  // Hints for planning and optimization
  Target [] getTargetList();
  boolean hasGroupByClause();
}