/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation.filter;

import java.beans.ConstructorProperties;

import cascading.flow.FlowProcess;
import cascading.operation.ConcreteCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class Or is a {@link Filter} class that will logically 'or' the results of the constructor provided Filter
 * instances.
 * <p/>
 * Logically, if {@link Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)} returns {@code true} for any of the given instances,
 * this filter will return {@code true}.
 *
 * @see And
 * @see Xor
 * @see Not
 */
public class Or extends Logic
  {
  /**
   * Constructor Or creates a new Or instance where all Filter instances receive all arguments.
   *
   * @param filters of type Filter...
   */
  @ConstructorProperties({"filters"})
  public Or( Filter... filters )
    {
    super( filters );
    }

  /**
   * Constructor Or creates a new Or instance.
   *
   * @param lhsArgumentSelector of type Fields
   * @param lhsFilter           of type Filter
   * @param rhsArgumentSelector of type Fields
   * @param rhsFilter           of type Filter
   */
  @ConstructorProperties({"lhsArgumentsSelector", "lhsFilter", "rhsArgumentSelector", "rhsFilter"})
  public Or( Fields lhsArgumentSelector, Filter lhsFilter, Fields rhsArgumentSelector, Filter rhsFilter )
    {
    super( lhsArgumentSelector, lhsFilter, rhsArgumentSelector, rhsFilter );
    }

  /**
   * Constructor Or creates a new Or instance.
   *
   * @param argumentSelectors of type Fields[]
   * @param filters           of type Filter[]
   */
  @ConstructorProperties({"argumentSelectors", "filters"})
  public Or( Fields[] argumentSelectors, Filter[] filters )
    {
    super( argumentSelectors, filters );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    TupleEntry arguments = filterCall.getArguments();
    Context context = (Context) filterCall.getContext();

    TupleEntry[] argumentEntries = context.argumentEntries;
    Object[] contexts = context.contexts;

    try
      {
      for( int i = 0; i < argumentSelectors.length; i++ )
        {
        TupleEntry entry = argumentEntries[ i ];

        entry.setTuple( arguments.selectTuple( argumentSelectors[ i ] ) );

        ( (ConcreteCall) filterCall ).setArguments( entry );
        filterCall.setContext( contexts[ i ] );

        if( filters[ i ].isRemove( flowProcess, filterCall ) )
          return true;
        }

      return false;
      }
    finally
      {
      ( (ConcreteCall) filterCall ).setArguments( arguments );
      filterCall.setContext( context );
      }
    }
  }