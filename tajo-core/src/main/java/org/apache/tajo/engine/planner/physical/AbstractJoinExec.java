package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.worker.TaskAttemptContext;

public abstract class AbstractJoinExec extends BinaryPhysicalExec {

  public AbstractJoinExec(final TaskAttemptContext context, final JoinNode plan,
                          final PhysicalExec outer, PhysicalExec inner) {
    super(context, plan.getInSchema(), plan.getOutSchema(), outer, inner);
  }
}
