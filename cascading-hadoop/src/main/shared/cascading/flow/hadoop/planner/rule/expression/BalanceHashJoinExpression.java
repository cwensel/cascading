package cascading.flow.hadoop.planner.rule.expression;

import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.expression.FlowElementExpression;
import cascading.flow.planner.iso.expression.PathScopeExpression;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.expressiongraph.SyncPipeExpressionGraph;
import cascading.pipe.HashJoin;
import cascading.tap.hadoop.Hfs;

public class BalanceHashJoinExpression extends RuleExpression {

  public BalanceHashJoinExpression() {
    super(
      new SyncPipeExpressionGraph(),
      new ExpressionGraph()
        .arc(
          new FlowElementExpression(ElementCapture.Primary, Hfs.class),
          PathScopeExpression.BLOCKING,
          new FlowElementExpression(HashJoin.class)
        )
    );
  }
}
