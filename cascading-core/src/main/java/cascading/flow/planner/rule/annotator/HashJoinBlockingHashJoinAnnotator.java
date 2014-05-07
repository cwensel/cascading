/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.flow.planner.rule.annotator;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.rule.PlanPhase;
import cascading.flow.planner.rule.expression.NonBlockedBlockedJoinJoinAnnotatorExpression;
import cascading.flow.planner.rule.transformer.RuleAnnotationTransformer;
import cascading.flow.stream.annotations.BlockingMode;

/**
 *
 */
public class HashJoinBlockingHashJoinAnnotator extends RuleAnnotationTransformer
  {
  public HashJoinBlockingHashJoinAnnotator()
    {
    super(
      PlanPhase.PostResolveAssembly,
      new NonBlockedBlockedJoinJoinAnnotatorExpression(),
      new ElementAnnotation( ElementExpression.Capture.Secondary, BlockingMode.Blocked )
    );
    }

  }
