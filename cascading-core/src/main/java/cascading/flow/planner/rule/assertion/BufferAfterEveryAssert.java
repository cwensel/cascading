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
import cascading.flow.planner.rule.expression.BufferAfterEveryExpression;

import static cascading.flow.planner.rule.PlanPhase.PreBalanceAssembly;

/**
 *
 */
public class BufferAfterEveryAssert extends RuleAssert
  {
  public BufferAfterEveryAssert()
    {
    super(
      PreBalanceAssembly,
      new BufferAfterEveryExpression(),
      "only one Every with a Buffer may follow a GroupBy or CoGroup pipe, no other Every instances are allowed immediately before or after, found: {Primary} before: {Secondary}"
    );
    }
  }
