/**
 * 
 */
package nta.engine.planner.global;

/**
 * @author jihoon
 *
 */
public enum OpType {
	NULL,
	SCAN, 
	SELECTION, 
	PROJECTION, 
	SORT,
	JOIN, 
	LOCAL_GROUP_BY,
	MERGE_GROUP_BY,
	RENAME, 
	SET_UNION, 
	SET_DIFF, 
	SET_INTERSECT,
	CREATE_TABLE,
	INSERT_INTO,
	SHOW_TABLE,
	SHOW_FUNCTION,
	DESC_TABLE,
}
