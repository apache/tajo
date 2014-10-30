package org.apache.tajo.plan.rewrite.rules;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.logical.*;
import org.apache.tajo.plan.logical.SortNode.SortRangeType;
import org.apache.tajo.plan.rewrite.RewriteRule;
import org.apache.tajo.plan.util.PlannerUtil;

public class AddSortForInsertRewriter implements RewriteRule {
  private int[] sortColumnIndexes;

  public AddSortForInsertRewriter(OverridableConf queryContext, TableDesc tableDesc, Column[] sortColumns) {
    this.sortColumnIndexes = new int[sortColumns.length];

    Schema tableSchema = tableDesc.getSchema();
    for (int i = 0; i < sortColumns.length; i++) {
      sortColumnIndexes[i] = tableSchema.getColumnId(sortColumns[i].getQualifiedName());
    }
  }

  @Override
  public String getName() {
    return "AddSortForInsertRewriter";
  }

  @Override
  public boolean isEligible(LogicalPlan plan) {
    StoreType storeType = PlannerUtil.getStoreType(plan);

    return storeType != null;
  }

  @Override
  public LogicalPlan rewrite(LogicalPlan plan) throws PlanningException {
    LogicalRootNode rootNode = plan.getRootBlock().getRoot();
    UnaryNode topNode = rootNode.getChild();

    SortNode sortNode = plan.createNode(SortNode.class);
    sortNode.setSortRangeType(SortRangeType.USING_STORAGE_MANAGER);
    sortNode.setInSchema(topNode.getInSchema());
    sortNode.setOutSchema(topNode.getInSchema());

    SortSpec[] sortSpecs = new SortSpec[sortColumnIndexes.length];
    for (int i = 0; i < sortColumnIndexes.length; i++) {
      sortSpecs[i] = new SortSpec(topNode.getInSchema().getColumn(sortColumnIndexes[i]), true, true);
    }
    sortNode.setSortSpecs(sortSpecs);

    sortNode.setChild(topNode.getChild());
    topNode.setChild(sortNode);
    plan.getRootBlock().registerNode(sortNode);

    return plan;
  }
}
