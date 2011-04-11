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

package cascading.operation.assertion;

import cascading.flow.FlowProcess;
import cascading.operation.ValueAssertion;
import cascading.operation.ValueAssertionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/** Class AssertNull asserts that every value in the argument values {@link Tuple} is a null value. */
public class AssertNull extends BaseAssertion implements ValueAssertion
  {

  /** Constructor AssertNull creates a new AssertNull instance. */
  public AssertNull()
    {
    super( "argument '%s' value was not null, in tuple: %s" );
    }

  /** @see cascading.operation.ValueAssertion#doAssert(cascading.flow.FlowProcess, cascading.operation.ValueAssertionCall) */
  public void doAssert( FlowProcess flowProcess, ValueAssertionCall assertionCall )
    {
    TupleEntry input = assertionCall.getArguments();
    int pos = 0;

    for( Object value : input.getTuple() )
      {
      if( value != null )
        fail( input.getFields().get( pos ), input.getTuple().print() );

      pos++;
      }

    }
  }