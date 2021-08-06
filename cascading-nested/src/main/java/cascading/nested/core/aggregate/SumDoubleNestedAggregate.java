/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.core.aggregate;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;

/**
 * Class SumDoubleNestedAggregate is a {@link cascading.nested.core.NestedAggregate} implementation for summing
 * double values collected from the parent container object.
 */
public class SumDoubleNestedAggregate<Node> extends BaseNumberNestedAggregate<Node, Double, BaseNumberNestedAggregate.BaseContext<Double, Node>>
  {
  public static class Context<Node> extends BaseContext<Double, Node>
    {
    double sum = 0D;

    public Context( BaseNumberNestedAggregate<Node, Double, BaseContext<Double, Node>> aggregateFunction, CoercibleType<Node> coercibleType )
      {
      super( aggregateFunction, coercibleType );
      }

    @Override
    protected void aggregateFilteredValue( Double value )
      {
      if( value == null )
        return;

      sum += value;
      }

    @Override
    protected void completeAggregateValue( Tuple results )
      {
      results.set( 0, sum );
      }

    @Override
    public void reset()
      {
      sum = 0D;
      super.reset();
      }
    }

  public SumDoubleNestedAggregate( Fields declaredFields )
    {
    super( declaredFields, Double.TYPE );
    }

  @Override
  public Context<Node> createContext( CoercibleType<Node> coercibleType )
    {
    return new Context<>( this, coercibleType );
    }
  }
