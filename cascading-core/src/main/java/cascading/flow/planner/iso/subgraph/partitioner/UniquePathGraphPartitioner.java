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

package cascading.flow.planner.iso.subgraph.partitioner;

import cascading.flow.planner.iso.ElementAnnotation;
import cascading.flow.planner.iso.expression.ExpressionGraph;
import cascading.flow.planner.iso.subgraph.SubGraphIterator;
import cascading.flow.planner.iso.subgraph.iterator.ExpressionSubGraphIterator;
import cascading.flow.planner.iso.subgraph.iterator.IncludeRemainderSubGraphIterator;
import cascading.flow.planner.iso.subgraph.iterator.UniquePathSubGraphIterator;

/**
 *
 */
public class UniquePathGraphPartitioner extends ExpressionGraphPartitioner
  {
  boolean includeRemainders = false;

  public UniquePathGraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, ElementAnnotation... annotations )
    {
    super( contractionGraph, expressionGraph, annotations );
    }

  public UniquePathGraphPartitioner( ExpressionGraph contractionGraph, ExpressionGraph expressionGraph, boolean includeRemainders, ElementAnnotation... annotations )
    {
    super( contractionGraph, expressionGraph, annotations );
    this.includeRemainders = includeRemainders;
    }

  @Override
  protected SubGraphIterator wrapIterator( ExpressionSubGraphIterator expressionIterator )
    {
    if( !includeRemainders )
      return new UniquePathSubGraphIterator( expressionIterator, true );

    return new IncludeRemainderSubGraphIterator( new UniquePathSubGraphIterator( expressionIterator, true ) );
    }
  }
