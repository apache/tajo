package nta.engine.plan;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public enum NodeType {
	Save,
	Load,
	GroupBy,
	Project,
	Union,
	Intersect,
	Except,
	Select,
	Scan,
	Sort,
	Limit,
	Join,
	Result,
	SessionClear
}
