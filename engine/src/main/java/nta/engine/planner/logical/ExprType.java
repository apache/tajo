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
	RECEIVE,
	RENAME,
	ROOT,
	SCAN,
	SELECTION,
	SEND,
	SET_DIFF, 
	SET_UNION,
  SET_INTERSECT,
	SHOW_TABLE,
	SHOW_FUNCTION,
	SORT,
	STORE
}
