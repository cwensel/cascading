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
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  private void assertFail( GroupAssertion assertion, TupleEntry groupEntry, TupleEntry... values )
    {
    ConcreteCall operationCall = new ConcreteCall();

    operationCall.setGroup( groupEntry );

    assertion.prepare( FlowProcess.NULL, operationCall );
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

    assertion.prepare( FlowProcess.NULL, operationCall );
    assertion.start( FlowProcess.NULL, operationCall );

    for( TupleEntry value : values )
      {
      operationCall.setArguments( value );
      assertion.aggregate( FlowProcess.NULL, operationCall );
      }

    operationCall.setArguments( null );
    assertion.doAssert( FlowProcess.NULL, operationCall );

    assertion.cleanup( FlowProcess.NULL, operationCall );
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