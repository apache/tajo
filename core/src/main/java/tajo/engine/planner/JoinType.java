package tajo.engine.planner;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public enum JoinType {
  CROSS_JOIN,
  INNER,
	LEFT_OUTER,
	RIGHT_OUTER,
	FULL_OUTER,
  UNION
}
