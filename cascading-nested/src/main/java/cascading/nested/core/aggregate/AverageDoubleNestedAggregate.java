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

package cascading.nested.core.aggregate;

import java.beans.ConstructorProperties;
import java.util.function.Consumer;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;

/**
 * Class AverageDoubleNestedAggregate is a @{link cascading.nested.core.NestedAggregate} implementation for averaging
 * the elements collected from the parent container object.
 * <p>
 * Optionally null values can be ignored or counted against the average.
 *
 * @param <Node>
 */
public class AverageDoubleNestedAggregate<Node> extends BaseNumberNestedAggregate<Node, Double, BaseNumberNestedAggregate.BaseContext<Double, Node>>
  {
  public enum Include
    {
      ALL,
      NO_NULLS
    }

  public static class Context<Node> extends BaseContext<Double, Node>
    {
    final Consumer<Double> aggregate;

    int count = 0;
    double sum = 0D;

    public Context( BaseNumberNestedAggregate<Node, Double, BaseContext<Double, Node>> aggregateFunction, CoercibleType<Node> coercibleType, Include include )
      {
      super( aggregateFunction, coercibleType );

      switch( include )
        {
        case ALL:
          aggregate = this::aggregateAll;
          break;
        case NO_NULLS:
          aggregate = this::aggregateNoNulls;
          break;
        default:
          throw new IllegalArgumentException( "unknown include type, got: " + include );
        }
      }

    @Override
    protected void aggregateFilteredValue( Double value )
      {
      aggregate.accept( value );
      }

    protected void aggregateNoNulls( Double value )
      {
      if( value == null )
        return;

      count++;
      sum += value;
      }

    protected void aggregateAll( Double value )
      {
      count++;

      if( value == null )
        return;

      sum += value;
      }

    @Override
    protected void completeAggregateValue( Tuple results )
      {
      results.set( 0, sum / count );
      }

    @Override
    public void reset()
      {
      count = 0;
      sum = 0D;
      super.reset();
      }
    }

  final protected Include include;

  /**
   * @param declaredFields
   */
  @ConstructorProperties({"declaredFields"})
  public AverageDoubleNestedAggregate( Fields declaredFields )
    {
    this( declaredFields, Include.ALL );
    }

  @ConstructorProperties({"declaredFields", "include"})
  public AverageDoubleNestedAggregate( Fields declaredFields, Include include )
    {
    super( declaredFields, Double.TYPE );
    this.include = include;
    }

  @Override
  protected boolean discardNullValues()
    {
    return false;
    }

  @Override
  public Context<Node> createContext( CoercibleType<Node> nestedCoercibleType )
    {
    return new Context<>( this, nestedCoercibleType, include );
    }
  }
