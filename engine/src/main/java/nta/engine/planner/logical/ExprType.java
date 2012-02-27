/**
 * 
 */
package nta.engine.planner.logical;

/**
 * @author Hyunsik Choi
 *
 */
public enum ExprType {
  CREATE_INDEX,
  CREATE_TABLE,	
	DESC_TABLE,
	EXPRS,
	GROUP_BY,
	INSERT_INTO,
	JOIN,
	PROJECTION,
	RENAME,
	ROOT,
	SCAN,
	SELECTION,
	SET_DIFF, 
	SET_UNION,
  SET_INTERSECT,
	SHOW_TABLE,
	SHOW_FUNCTION,
	SORT,
	STORE
}
