/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.planner.rule.annotator;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementCapture;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.expression.LogicalMergeAnnotatorExpression;
import cascading.flow.planner.rule.transformer.RuleAnnotationTransformer;
import cascading.flow.stream.annotations.RoleMode;
import cascading.pipe.Merge;

/**
 * A logic merge is where a {@link Merge} merges the same input, and subsequently node boundaries do not need to be
 * traversed
 */
public class LogicalMergeAnnotator extends RuleAnnotationTransformer
  {
  public LogicalMergeAnnotator()
    {
    super(
      PlanPhase.PreResolveAssembly,
      new LogicalMergeAnnotatorExpression(),
      new ElementAnnotation( ElementCapture.Secondary, RoleMode.Logical )
    );
    }
  }
