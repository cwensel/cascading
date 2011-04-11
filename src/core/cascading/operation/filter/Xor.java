/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
 * Class Xor is a {@link Filter} class that will logically 'xor' (exclusive or) the results of the
 * constructor provided Filter instances.
 * <p/>
 * Logically, if {@link Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)} returns {@code true} for all given instances,
 * or returns {@code false} for all given instances, this filter will return {@code true}.
 * <p/>
 * Note that Xor can only be applied to two values.
 *
 * @see And
 * @see Or
 * @see Not
 */
public class Xor extends Logic
  {
  /**
   * Constructor Xor creates a new Xor instance where all Filter instances receive all arguments.
   *
   * @param filters of type Filter...
   */
  @ConstructorProperties({"filters"})
  public Xor( Filter... filters )
    {
    super( filters );
    }

  /**
   * Constructor Xor creates a new Xor instance.
   *
   * @param lhsArgumentSelector of type Fields
   * @param lhsFilter           of type Filter
   * @param rhsArgumentSelector of type Fields
   * @param rhsFilter           of type Filter
   */
  @ConstructorProperties({"lhsArgumentsSelector", "lhsFilter", "rhsArgumentSelector", "rhsFilter"})
  public Xor( Fields lhsArgumentSelector, Filter lhsFilter, Fields rhsArgumentSelector, Filter rhsFilter )
    {
    super( lhsArgumentSelector, lhsFilter, rhsArgumentSelector, rhsFilter );
    }

  /** @see cascading.operation.Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall) */
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    Context context = (Logic.Context) filterCall.getContext();
    TupleEntry[] argumentEntries = context.argumentEntries;
    Object[] contexts = context.contexts;

    TupleEntry lhsEntry = argumentEntries[ 0 ];
    TupleEntry rhsEntry = argumentEntries[ 1 ];

    lhsEntry.setTuple( filterCall.getArguments().selectTuple( argumentSelectors[ 0 ] ) );
    rhsEntry.setTuple( filterCall.getArguments().selectTuple( argumentSelectors[ 1 ] ) );

    filterCall.setContext( contexts[ 0 ] );
    boolean lhsResult = filters[ 0 ].isRemove( flowProcess, filterCall );

    filterCall.setContext( contexts[ 1 ] );
    boolean rhsResult = filters[ 1 ].isRemove( flowProcess, filterCall );

    try
      {
      return lhsResult != rhsResult;
      }
    finally
      {
      filterCall.setContext( context );
      }
    }
  }