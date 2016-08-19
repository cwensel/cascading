package cascading.flow.planner.rule.transformer;

import cascading.flow.planner.iso.transformer.ReplaceGraphFactoryBasedTransformer;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.RuleExpression;
import cascading.flow.planner.rule.RuleTransformer;

/**
 * RuleTransformer that uses the supplied expression to fetch the FlowElement to replace.
 * The replacement FlowElement is created using the supplied factory.
 */
public class RuleReplaceFactoryBasedTransformer extends RuleTransformer {

  public RuleReplaceFactoryBasedTransformer(
    PlanPhase phase,
    RuleExpression ruleExpression,
    String factoryName) {

    super(phase, ruleExpression);

    // the way we've set up our HashJoinExpression, we should have the contractedTransformer
    // set up but not the subGraph transformer
    if (subGraphTransformer != null) {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(subGraphTransformer,
        ruleExpression.getMatchExpression(), factoryName);
    } else if (contractedTransformer != null) {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(contractedTransformer,
        ruleExpression.getMatchExpression(), factoryName);
    } else {
      graphTransformer = new ReplaceGraphFactoryBasedTransformer(null,
        ruleExpression.getMatchExpression(), factoryName);
    }
  }
}
