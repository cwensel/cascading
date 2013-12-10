/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class Xor is a {@link Filter} class that will logically 'xor' (exclusive or) the results of the
 * constructor provided Filter instances.
 * <p/>
 * Logically, if {@link Filter#isRemove(cascading.flow.FlowProcess, cascading.operation.FilterCall)} returns {@code true} for all given instances,
 * or returns {@code false} for all given instances, this filter will return {@code false}.
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

  @Override
  public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
    {
    Context context = (Logic.Context) filterCall.getContext();

    TupleEntry lhsEntry = context.argumentEntries[ 0 ];
    TupleEntry rhsEntry = context.argumentEntries[ 1 ];

    lhsEntry.setTuple( filterCall.getArguments().selectTuple( argumentSelectors[ 0 ] ) );
    rhsEntry.setTuple( filterCall.getArguments().selectTuple( argumentSelectors[ 1 ] ) );

    boolean lhsResult = filters[ 0 ].isRemove( flowProcess, context.calls[ 0 ] );
    boolean rhsResult = filters[ 1 ].isRemove( flowProcess, context.calls[ 1 ] );

    return lhsResult != rhsResult;
    }
  }