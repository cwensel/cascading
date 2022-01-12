/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.flow.hadoop.planner.rule.transformer;

import cascading.flow.hadoop.planner.rule.expression.AccumulatedTapOnHashJoinExpression;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.flow.planner.rule.transformer.RuleReplaceFactoryBasedTransformer;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 * The ReplaceAccumulateTapWithDistCacheTransformer will decorate all temp or checkpoint taps on the accumulated side
 * of a {@link cascading.pipe.HashJoin} with a {@link cascading.tap.hadoop.DistCacheTap} instance that wraps the
 * original Tap instance.
 */
public class ReplaceAccumulateTapWithDistCacheTransformer extends RuleReplaceFactoryBasedTransformer
  {
  public ReplaceAccumulateTapWithDistCacheTransformer()
    {
    super(
      BalanceAssembly,
      new AccumulatedTapOnHashJoinExpression(),
      IntermediateTapElementFactory.ACCUMULATED_TAP
    );
    }
  }
