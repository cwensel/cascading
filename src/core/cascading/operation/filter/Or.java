/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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