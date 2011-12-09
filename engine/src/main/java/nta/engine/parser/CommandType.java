/**
 * 
 */
package nta.engine.parser;

/**
 * @author Hyunsik Choi
 *
 */
public enum CommandType {
	// Select
	SELECT,
	
	// Update
	INSERT,
	UPDATE,
	DELETE,
	
	// Schema
	CREATE_TABLE,
	DROP_TABLE,
	
	// Control
	SHOW_TABLES,
	DESC_TABLE,
	SHOW_FUNCTION;
}
