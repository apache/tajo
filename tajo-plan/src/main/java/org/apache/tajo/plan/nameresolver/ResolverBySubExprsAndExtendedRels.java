//package org.apache.tajo.plan.nameresolver;
//
//import org.apache.tajo.algebra.ColumnReferenceExpr;
//import org.apache.tajo.catalog.Column;
//import org.apache.tajo.exception.AmbiguousColumnException;
//import org.apache.tajo.exception.AmbiguousTableException;
//import org.apache.tajo.exception.UndefinedColumnException;
//import org.apache.tajo.exception.UndefinedTableException;
//import org.apache.tajo.plan.LogicalPlan;
//
//public class ResolverBySubExprsAndExtendedRels extends NameResolver {
//
//  @Override
//  public Column resolve(LogicalPlan plan, LogicalPlan.QueryBlock block, ColumnReferenceExpr columnRef)
//      throws AmbiguousColumnException, AmbiguousTableException, UndefinedColumnException, UndefinedTableException {
//
//    Column column = resolveFromCurrentAndChildNode(block, columnRef);
//    if (column == null) {
//      column = resolveFromRelsWithinBlock(plan, block, columnRef);
//    }
//
//    if (column == null) {
//      column = resolveFromAllSelfDescReslInAllBlocks(plan, block, columnRef);
//    }
//
//    if (column == null) {
//      throw new UndefinedColumnException(columnRef.getCanonicalName());
//    }
//    return column;
//  }
//}
