/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.hadoop.planner.rule.expression.BalanceGroupSplitJoinExpression;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.flow.planner.rule.transformer.RuleInsertionTransformer;

import static cascading.flow.planner.rule.PlanPhase.BalanceAssembly;

/**
 *
 */
public class TapBalanceGroupSplitJoinTransformer extends RuleInsertionTransformer
  {
  public TapBalanceGroupSplitJoinTransformer()
    {
    super(
      BalanceAssembly,
      new BalanceGroupSplitJoinExpression(),
      IntermediateTapElementFactory.TEMP_TAP
    );
    }
  }
