package cascading.flow.hadoop.planner.rule.transformer;

import cascading.flow.hadoop.planner.rule.expression.BalanceHashJoinExpression;
import cascading.flow.planner.rule.transformer.RuleReplaceFactoryBasedTransformer;
import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

public class BalanceHashJoinDistCacheTransformer extends RuleReplaceFactoryBasedTransformer {

  /* Key used for registering DistCacheTapElementFactory */
  public static String DIST_CACHE_TAP = "cascading.registry.tap.distcache";

  public BalanceHashJoinDistCacheTransformer() {
    super(
      BalanceAssembly,
      new BalanceHashJoinExpression(),
      DIST_CACHE_TAP
    );
  }
}
