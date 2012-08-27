/**
 * 
 */
package tajo.engine.planner.logical;

/**
 * @author Hyunsik Choi
 *
 */
public enum ExprType {
  BST_INDEX_SCAN,
  CREATE_INDEX,
  CREATE_TABLE,	
	DESC_TABLE,
	EXCEPT,
	EXPRS,
	GROUP_BY,
	INSERT_INTO,
	INTERSECT,
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
	STORE,
	UNION
}
