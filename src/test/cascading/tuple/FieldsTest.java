/*
 * Copyright (c) 2007-2008 Chris K Wensel. All Rights Reserved.
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

package cascading.tuple;

import cascading.CascadingTestCase;

/** @version $Id: //depot/calku/cascading/src/test/cascading/tuple/FieldsTest.java#2 $ */
public class FieldsTest extends CascadingTestCase
  {
  public FieldsTest()
    {
    super( "field tests" );
    }

  @Override
  protected void setUp() throws Exception
    {
    super.setUp();
    }

  public void testAppend()
    {
    Fields fieldA = new Fields( 0, 1 );
    Fields fieldB = new Fields( 0 );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", 2, appended.get( 2 ) );
    }

  public void testAppendNamed()
    {
    Fields fieldA = new Fields( 0, 1 );
    Fields fieldB = new Fields( "a" );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", "a", appended.get( 2 ) );
    }

  public void testAppendNamed2()
    {
    Fields fieldA = new Fields( "a" );
    Fields fieldB = new Fields( 0, 1 );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", "a", appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", 2, appended.get( 2 ) );
    }

  public void testDiff()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "a" );

    Fields diff = fieldA.minus( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "b", diff.get( 0 ) );
    }

  public void testDiffDupe()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "a", "a" );

    Fields diff = fieldA.minus( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "b", diff.get( 0 ) );
    }

  public void testDiffSame()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "b", "a" );

    Fields diff = fieldA.minus( fieldB );

    assertEquals( "not equal: ", 0, diff.size() );
    }

  public void testDiffIndex()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 0 );

    Fields diff = fieldA.minus( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "b", diff.get( 0 ) );
    }

  public void testDiffIndex2()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 1 );

    Fields diff = fieldA.minus( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "a", diff.get( 0 ) );
    }

  public void testDiff3()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 0, 4 );

    try
      {
      Fields diff = fieldA.minus( fieldB );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  /**
   * step -> step -> step
   * dec(sel) -> dec(sel)
   * a,b -> c,d  ->  d,f -> d,f,g
   * <p/>
   * the result of a select is always a field declaration
   */
  public void testSelect()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  public void testSelect2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( "b", "a" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  public void testSelectPos()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields selectA = new Fields( 0, 1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    assertEquals( "not equal: ", 1, got.get( 1 ) );
    }

  /** this one is funky. regardless of the input, positions are always monotonically increasing */
  public void testSelectPos2()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields selectA = new Fields( 1, 0 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    assertEquals( "not equal: ", 1, got.get( 1 ) );
    }

  public void testSelectMixed()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( 0, 1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  public void testSelectMixed2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( 1, 0 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  public void testSelectMixed3()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( 1, 0, 2, 3 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    assertEquals( "not equal: ", 2, got.get( 2 ) );
    assertEquals( "not equal: ", 3, got.get( 3 ) );
    }

  public void testSelectMixed4()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( 2, 3, "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    assertEquals( "not equal: ", 1, got.get( 1 ) );
    assertEquals( "not equal: ", "a", got.get( 2 ) );
    assertEquals( "not equal: ", "b", got.get( 3 ) );
    }

  public void testSelectMixedNeg()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( -2, -1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  public void testSelectMixedNeg2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( -1, -2 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  public void testSelectMixedNeg3()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( 1, 0, -2, -1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    assertEquals( "not equal: ", 2, got.get( 2 ) );
    assertEquals( "not equal: ", 3, got.get( 3 ) );
    }

  public void testSelectMixedNeg4()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( -2, -1, "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    assertEquals( "not equal: ", 1, got.get( 1 ) );
    assertEquals( "not equal: ", "a", got.get( 2 ) );
    assertEquals( "not equal: ", "b", got.get( 3 ) );
    }

  public void testSelectMixedNeg5()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( -4, -3, -2, -1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    assertEquals( "not equal: ", 2, got.get( 2 ) );
    assertEquals( "not equal: ", 3, got.get( 3 ) );
    }

  public void testResolveIndexOnly()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 0 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    }

  public void testResolveIndexOnly2()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 1 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 1, got.get( 0 ) );
    }

  public void testResolveIndexOnly3()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 2 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 2, got.get( 0 ) );
    }

  public void testResolveIndexOnly4()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 3 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 3, got.get( 0 ) );
    }

  public void testResolveIndexAppended()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 0 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    }

  public void testResolveIndexAppended2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 1 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    }

  public void testResolveIndexAppended3()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 2 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "c", got.get( 0 ) );
    }

  public void testResolveIndexAppended4()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 3 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "d", got.get( 0 ) );
    }

  public void testResolveAppended()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "a" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    }

  public void testResolveAppended2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "b" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    }

  public void testResolveAppended3()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "c" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "c", got.get( 0 ) );
    }

  public void testResolveAppended4()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "d" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "d", got.get( 0 ) );
    }

  public void testResolveAppended5()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "a", "d" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "d", got.get( 1 ) );
    }

  public void testResolveAppendedComplex()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields declarationB = new Fields( "c", "d", 2, 3 );
    Fields selectA = new Fields( -4, -3, -2, -1, 0, 1, 2, 3 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 8, got.size() );
    assertEquals( "not equal: ", "c", got.get( 0 ) );
    assertEquals( "not equal: ", "d", got.get( 1 ) );
    assertEquals( "not equal: ", -2, got.get( 2 ) );
    assertEquals( "not equal: ", -1, got.get( 3 ) );
    assertEquals( "not equal: ", "a", got.get( 4 ) );
    assertEquals( "not equal: ", "b", got.get( 5 ) );
    assertEquals( "not equal: ", 2, got.get( 6 ) );
    assertEquals( "not equal: ", 3, got.get( 7 ) );
    }

  public void testResolveAppendedFail()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields declarationB = new Fields( "c", "d", 2, 3 );
    Fields selectA = new Fields( -4, -3, "a", -1, 0, 1, 2, 3 );

    try
      {
      Fields.resolve( selectA, declarationA, declarationB );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      }
    }

  /** This must fail, no way the dec is a declaration */
  public void testSelectFail()
    {
    Fields declarationA = new Fields( 2, "a", "b", 3 );
    Fields selectA = new Fields( 2, 3, "a", "b" );

    try
      {
      declarationA.select( selectA );
      fail( "did not catch invalid field" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testSelectWildcard()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = Fields.ALL;

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  public void testSelectComplex()
    {
    Fields declarationA = new Fields( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields selectA = new Fields( "movie", "name1", "rate1", "name2", "rate2" );

    int[] got = declarationA.getPos( selectA );

    assertEquals( "not equal: ", 5, got.length );
    assertEquals( "not equal: ", 0, got[ 0 ] );
    assertEquals( "not equal: ", 1, got[ 1 ] );
    assertEquals( "not equal: ", 3, got[ 2 ] );
    assertEquals( "not equal: ", 4, got[ 3 ] );
    assertEquals( "not equal: ", 6, got[ 4 ] );
    }

  public void testUnknown()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = Fields.UNKNOWN;

    doTestUnknown( declarationA, declarationB );
    doTestUnknown( declarationB, declarationA );
    }

  private void doTestUnknown( Fields declarationA, Fields declarationB )
    {
    Fields got = declarationA.append( declarationB );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    assertEquals( "not equal: ", 0, got.getPos( new Fields( "a" ) )[ 0 ] );
    assertEquals( "not equal: ", 1, got.getPos( new Fields( "b" ) )[ 0 ] );
    assertEquals( "not equal: ", 3, got.getPos( new Fields( 3 ) )[ 0 ] );

    try
      {
      got.getPos( new Fields( "c" ) );
      fail( "did not throw failure" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testSelectNegative()
    {
    Fields declarationA = new Fields( "movie", "name1", "movie1", "rate1", "name2", "movie2", "rate2" );
    Fields selectA = new Fields( -1 );

    int[] got = declarationA.getPos( selectA );

    assertEquals( "not equal: ", 1, got.length );
    assertEquals( "not equal: ", 6, got[ 0 ] );

    Fields selectB = new Fields( -8 );

    try
      {
      got = declarationA.getPos( selectB );
      fail( "did not throw failure" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  public void testJoin()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "c", "d" );

    Fields join = Fields.join( fieldA, fieldB );

    assertEquals( "not equal: ", 4, join.size() );
    assertEquals( "not equal: ", "a", join.get( 0 ) );
    assertEquals( "not equal: ", "b", join.get( 1 ) );
    assertEquals( "not equal: ", "c", join.get( 2 ) );
    assertEquals( "not equal: ", "d", join.get( 3 ) );
    }


  }
