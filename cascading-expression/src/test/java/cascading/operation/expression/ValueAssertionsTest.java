/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.operation.expression;

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.AssertionException;
import cascading.operation.ConcreteCall;
import cascading.operation.ValueAssertion;
import cascading.operation.assertion.AssertExpression;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.junit.Test;

/**
 *
 */
public class ValueAssertionsTest extends CascadingTestCase
  {
  public ValueAssertionsTest()
    {
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  private ConcreteCall getOperationCall( TupleEntry tupleEntry )
    {
    ConcreteCall operationCall = new ConcreteCall( tupleEntry.getFields() );
    operationCall.setArguments( tupleEntry );
    return operationCall;
    }

  private void assertFail( ValueAssertion assertion, TupleEntry tupleEntry )
    {
    ConcreteCall concreteCall = getOperationCall( tupleEntry );
    assertion.prepare( FlowProcess.NULL, concreteCall );
    try
      {
      assertion.doAssert( FlowProcess.NULL, concreteCall );
      fail();
      }
    catch( AssertionException exception )
      {
      // do nothing
      }
    }

  private void assertPass( ValueAssertion assertion, TupleEntry tupleEntry )
    {
    ConcreteCall concreteCall = getOperationCall( tupleEntry );
    assertion.prepare( FlowProcess.NULL, concreteCall );
    assertion.doAssert( FlowProcess.NULL, concreteCall );
    }

  @Test
  public void testExpression()
    {
    ValueAssertion assertion = new AssertExpression( "$0 == 1", int.class );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "1", 0 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }
  }
