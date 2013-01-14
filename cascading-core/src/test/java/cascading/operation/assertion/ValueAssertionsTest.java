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

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.AssertionException;
import cascading.operation.ConcreteCall;
import cascading.operation.ValueAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

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

  private ConcreteCall getOperationCall( TupleEntry tupleEntry )
    {
    ConcreteCall operationCall = new ConcreteCall( tupleEntry.getFields() );
    operationCall.setArguments( tupleEntry );
    return operationCall;
    }

  private void assertPass( ValueAssertion assertion, TupleEntry tupleEntry )
    {
    ConcreteCall concreteCall = getOperationCall( tupleEntry );
    assertion.prepare( FlowProcess.NULL, concreteCall );
    assertion.doAssert( FlowProcess.NULL, concreteCall );
    }

  public void testNotNull()
    {
    ValueAssertion assertion = new AssertNotNull();

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testNull()
    {
    ValueAssertion assertion = new AssertNull();

    assertFail( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) ); // all values must be null
    assertPass( assertion, getEntry( new Tuple( null, null ) ) );
    }

  public void testEquals()
    {
    ValueAssertion assertion = new AssertEquals( 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );

    assertion = new AssertEquals( "one", "two" );

    assertPass( assertion, getEntry( new Tuple( "one", "two" ) ) );
    assertFail( assertion, getEntry( new Tuple( null, null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testNotEquals()
    {
    ValueAssertion assertion = new AssertNotEquals( 4 );

    assertFail( assertion, getEntry( new Tuple( 4 ) ) );
    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ) );

    assertion = new AssertNotEquals( "one1", "two1" );

    assertFail( assertion, getEntry( new Tuple( "one1", "two1" ) ) );
    assertPass( assertion, getEntry( new Tuple( "one", "two" ) ) );
    assertPass( assertion, getEntry( new Tuple( null, null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testEqualsAll()
    {
    ValueAssertion assertion = new AssertEqualsAll( 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testMatches()
    {
    // match tuple, assert match
    ValueAssertion assertion = new AssertMatches( "^1$" );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "1" ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );

    // match tuple, assert match
    assertion = new AssertMatches( "^1$", false );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "1" ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );

    // match tuple, negate the match
    assertion = new AssertMatches( "^1$", true );

    assertFail( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "1" ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ) );

    }

  public void testMatchesAll()
    {
    // match elements, assert match
    ValueAssertion assertion = new AssertMatchesAll( "^1$" );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "1" ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 0, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );

    // match elements, assert match
    assertion = new AssertMatchesAll( "^1$", false );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "1" ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 0, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );

    // match elements, negate the match
    assertion = new AssertMatchesAll( "^1$", true );

    assertFail( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "1" ) ) );
    assertFail( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) ); // one of the values matches
    assertPass( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testExpression()
    {
    ValueAssertion assertion = new AssertExpression( "$0 == 1", int.class );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "1", 0 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testTupleEquals()
    {
    ValueAssertion assertion = new AssertSizeEquals( 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testTupleLessThan()
    {
    ValueAssertion assertion = new AssertSizeLessThan( 2 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testTupleMoreThan()
    {
    ValueAssertion assertion = new AssertSizeMoreThan( 1 );

    assertFail( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ) );
    }
  }
