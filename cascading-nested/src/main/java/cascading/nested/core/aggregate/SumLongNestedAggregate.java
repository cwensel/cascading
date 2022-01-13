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

import java.beans.ConstructorProperties;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;

/**
 * Class SumLongNestedAggregate is a {@link cascading.nested.core.NestedAggregate} implementation for summing
 * long values collected from the parent container object.
 */
public class SumLongNestedAggregate<Node> extends BaseNumberNestedAggregate<Node, Long, BaseNumberNestedAggregate.BaseContext<Long, Node>>
  {
  public static class Context<Node> extends BaseContext<Long, Node>
    {
    long sum = 0L;

    public Context( SumLongNestedAggregate<Node> aggregateFunction, CoercibleType<Node> coercibleType )
      {
      super( aggregateFunction, coercibleType );
      }

    @Override
    protected void aggregateFilteredValue( Long value )
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
      sum = 0L;
      super.reset();
      }
    }

  @ConstructorProperties({"declaredFields"})
  public SumLongNestedAggregate( Fields declaredFields )
    {
    super( declaredFields, Long.TYPE );
    }

  @Override
  public Context<Node> createContext( CoercibleType<Node> nestedCoercibleType )
    {
    return new Context<>( this, nestedCoercibleType );
    }
  }
