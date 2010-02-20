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
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class And is a {@link Filter} class that will logically 'and' the results of the constructor provided Filter
 * instances.
 * <p/>
 * Logically, if {@link Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall)} returns {@code true} for all given instances,
 * this filter will return {@code true}.
 *
 * @see Or
 * @see Xor
 * @see Not
 */
public class And extends Logic
  {
  /**
   * Constructor And creates a new And instance where all Filter instances receive all arguments.
   *
   * @param filters of type Filter...
   */
  @ConstructorProperties({"filters"})
  public And( Filter... filters )
    {
    super( filters );
    }

  /**
   * Constructor And creates a new And instance.
   *
   * @param lhsArgumentSelector of type Fields
   * @param lhsFilter           of type Filter
   * @param rhsArgumentSelector of type Fields
   * @param rhsFilter           of type Filter
   */
  @ConstructorProperties({"lhsArgumentsSelector", "lhsFilter", "rhsArgumentSelector", "rhsFilter"})
  public And( Fields lhsArgumentSelector, Filter lhsFilter, Fields rhsArgumentSelector, Filter rhsFilter )
    {
    super( lhsArgumentSelector, lhsFilter, rhsArgumentSelector, rhsFilter );
    }

  /**
   * Constructor And creates a new And instance.
   *
   * @param argumentSelectors of type Fields[]
   * @param filters           of type Filter[]
   */
  @ConstructorProperties({"argumentFilters", "filters"})
  public And( Fields[] argumentSelectors, Filter[] filters )
    {
    super( argumentSelectors, filters );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess,cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    Object[] contexts = (Object[]) filterCall.getContext();

    try
      {
      for( int i = 0; i < argumentSelectors.length; i++ )
        {
        TupleEntry entry = getArgumentEntries()[ i ];

        entry.setTuple( filterCall.getArguments().selectTuple( argumentSelectors[ i ] ) );

        filterCall.setContext( contexts[ i ] );

        if( !filters[ i ].isRemove( flowProcess, filterCall ) )
          return false;
        }

      return true;
      }
    finally
      {
      filterCall.setContext( contexts );
      }
    }
  }
