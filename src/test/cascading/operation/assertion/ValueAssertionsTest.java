/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingTestCase;
import cascading.flow.FlowProcess;
import cascading.operation.AssertionException;
import cascading.operation.OperationCall;
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
    super( "value assertions test" );
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  private void assertFail( ValueAssertion assertion, TupleEntry tupleEntry )
    {
    try
      {
      assertion.doAssert( FlowProcess.NULL, getOperationCall( tupleEntry ) );
      fail();
      }
    catch( AssertionException exception )
      {
      // do nothing
      }
    }

  private OperationCall getOperationCall( TupleEntry tupleEntry )
    {
    OperationCall operationCall = new OperationCall();
    operationCall.setArguments( tupleEntry );
    return operationCall;
    }

  private void assertPass( ValueAssertion assertion, TupleEntry tupleEntry )
    {
    assertion.doAssert( null, getOperationCall( tupleEntry ) );
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
    ValueAssertion assertion = new AssertExpression( "$0 == 1", Integer.class );

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
