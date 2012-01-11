/**
 * 
 */
package nta.engine.plan.logical;

/**
 * @author Hyunsik Choi
 *
 */
public enum OpType {
	SCAN, 
	SELECTION, 
	PROJECTION, 
	SORT, 
	JOIN, 
	GROUP_BY, 
	RENAME, 
	SET_UNION, 
	SET_DIFF, 
	SET_INTERSECT,
	CREATE_TABLE,
	INSERT_INTO,
	SHOW_TABLE,
	SHOW_FUNCTION,
	DESC_TABLE,
	ROOT
}
