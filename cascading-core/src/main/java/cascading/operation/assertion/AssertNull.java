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

  @Override
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