/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.planner.rule.assertion;

import cascading.flow.planner.rule.RuleAssert;
import cascading.flow.planner.rule.expression.LoneGroupExpression;

import static cascading.flow.planner.rule.PlanPhase.PrePartitionElements;

/**
 * Verifies that there are not only GroupAssertions following any given Group instance. This will adversely
 * affect the stream entering any subsequent Tap of Each instances.
 */
public class LoneGroupAssert extends RuleAssert
  {
  public LoneGroupAssert()
    {
    super(
      PrePartitionElements,
      new LoneGroupExpression(),
      "group assertions must be accompanied by aggregator operations"
    );
    }
  }
