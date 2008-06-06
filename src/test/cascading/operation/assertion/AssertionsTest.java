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
import cascading.operation.Assertion;
import cascading.operation.AssertionException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class AssertionsTest extends CascadingTestCase
  {
  public AssertionsTest()
    {
    super( "assetion test" );
    }

  private TupleEntry getEntry( Tuple tuple )
    {
    return new TupleEntry( Fields.size( tuple.size() ), tuple );
    }

  private void assertFail( Assertion assertion, TupleEntry tupleEntry )
    {
    try
      {
      assertion.doAssert( tupleEntry );
      fail();
      }
    catch( AssertionException exception )
      {
      // do nothing
      }
    }

  private void assertPass( Assertion assertion, TupleEntry tupleEntry )
    {
    assertion.doAssert( tupleEntry );
    }

  public void testNotNull()
    {
    Assertion assertion = new AssertNotNull();

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertPass( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testNull()
    {
    Assertion assertion = new AssertNull();

    assertFail( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) ); // all values must be null
    assertPass( assertion, getEntry( new Tuple( null, null ) ) );
    }

  public void testEquals()
    {
    Assertion assertion = new AssertEquals( 1 );

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
    Assertion assertion = new AssertEqualsAll( 1 );

    assertPass( assertion, getEntry( new Tuple( 1 ) ) );
    assertPass( assertion, getEntry( new Tuple( 1, 1, 1, 1, 1, 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( (Comparable) null ) ) );

    assertFail( assertion, getEntry( new Tuple( "0", 1 ) ) );
    assertFail( assertion, getEntry( new Tuple( "0", null ) ) );
    }

  public void testMatches()
    {
    // match tuple, assert match
    Assertion assertion = new AssertMatches( "^1$" );

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
    Assertion assertion = new AssertMatchesAll( "^1$" );

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

  }
