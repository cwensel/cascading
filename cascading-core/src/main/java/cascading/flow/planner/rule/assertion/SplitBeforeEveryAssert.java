/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.flow.planner.rule.assertion;

import cascading.flow.planner.rule.RuleAssert;
import cascading.flow.planner.rule.expression.SplitBeforeEveryExpression;

import static cascading.flow.planner.rule.PlanPhase.PreBalanceAssembly;

/**
 *
 */
public class SplitBeforeEveryAssert extends RuleAssert
  {
  public SplitBeforeEveryAssert()
    {
    super(
      PreBalanceAssembly,
      new SplitBeforeEveryExpression(),
      "Every instances may not split after a GroupBy or CoGroup pipe, found: {Primary} before: {Secondary}"
    );
    }
  }
