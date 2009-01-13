/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import cascading.operation.ConcreteCall;
import cascading.operation.GroupAssertion;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class GroupAssertionsTest extends CascadingTestCase
  {
  public GroupAssertionsTest()
    {
    super( "group assertions test" );
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  private void assertFail( GroupAssertion assertion, TupleEntry groupEntry, TupleEntry... values )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setGroup( groupEntry );

    assertion.start( FlowProcess.NULL, operationCall );

    for( TupleEntry value : values )
      {
      operationCall.setArguments( value );
      assertion.aggregate( FlowProcess.NULL, operationCall );
      }

    try
      {
      operationCall.setArguments( null );
      assertion.doAssert( FlowProcess.NULL, operationCall );
      fail();
      }
    catch( AssertionException exception )
      {
      //System.out.println( "exception.getMessage() = " + exception.getMessage() );
      // do nothing
      }
    }

  private void assertPass( GroupAssertion assertion, TupleEntry groupEntry, TupleEntry... values )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setGroup( groupEntry );

    assertion.start( null, operationCall );

    for( TupleEntry value : values )
      {
      operationCall.setArguments( value );
      assertion.aggregate( FlowProcess.NULL, operationCall );
      }

    operationCall.setArguments( null );
    assertion.doAssert( FlowProcess.NULL, operationCall );
    }

  public void testSizeEquals()
    {
    GroupAssertion assertion = new AssertGroupSizeEquals( 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );

    assertion = new AssertGroupSizeEquals( "1", 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );
    }

  public void testSizeLessThan()
    {
    GroupAssertion assertion = new AssertGroupSizeLessThan( 2 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );

    assertion = new AssertGroupSizeLessThan( "1", 2 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );
    }

  public void testSizeMoreThan()
    {
    GroupAssertion assertion = new AssertGroupSizeMoreThan( 1 );

    assertPass( assertion, getEntry( new Tuple( (Comparable) 1 ) ), getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ) );
    assertFail( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );

    assertion = new AssertGroupSizeMoreThan( "1", 1 );

    assertPass( assertion, getEntry( new Tuple( (Comparable) 1 ) ), getEntry( new Tuple( (Comparable) null ) ), getEntry( new Tuple( (Comparable) null ) ) );
    assertFail( assertion, getEntry( new Tuple( 1 ) ), getEntry( new Tuple( 1 ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ), getEntry( new Tuple( "0", null ) ) );
    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ), getEntry( new Tuple( "0", 1 ) ) );
    }

  }