/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.lang.reflect.Type;
import java.util.Arrays;

import cascading.CascadingTestCase;
import org.junit.Test;

import static cascading.tuple.Fields.names;
import static cascading.tuple.Fields.types;

public class FieldsTest extends CascadingTestCase
  {
  public FieldsTest()
    {
    }

  @Test
  public void testDuplicate()
    {
    try
      {
      new Fields( 0, 0 );
      fail( "accepted dupe field names" );
      }
    catch( Exception exception )
      {
      // do nothing
      }

    try
      {
      new Fields( "foo", "foo" );
      fail( "accepted dupe field names" );
      }
    catch( Exception exception )
      {
      // do nothing
      }
    }

  @Test
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

  @Test
  public void testAppend2()
    {
    Fields fieldA = new Fields( 0, 1 );
    Fields fieldB = new Fields( 2 );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", 2, appended.get( 2 ) );
    }

  @Test
  public void testAppend3()
    {
    Fields fieldA = Fields.NONE;
    Fields fieldB = new Fields( 2 );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 1, appended.size() );
    assertEquals( "not equal: ", 2, appended.get( 0 ) );
    }

  @Test
  public void testAppend4()
    {
    Fields fieldA = new Fields( 0, 1 );
    Fields fieldB = new Fields( -1 );

    Fields appended = fieldA.appendSelector( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", -1, appended.get( 2 ) );
    }

  @Test
  public void testAppend4Fail()
    {
    Fields fieldA = new Fields( 0, -1 );
    Fields fieldB = new Fields( -1 );

    try
      {
      Fields appended = fieldA.appendSelector( fieldB );
      fail();
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
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

  @Test
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

  @Test
  public void testAppendSelectNamed()
    {
    Fields fieldA = new Fields( 0, 1 );
    Fields fieldB = new Fields( "a" );

    Fields appended = fieldA.appendSelector( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", "a", appended.get( 2 ) );
    }

  @Test
  public void testAppendSelectNamed2()
    {
    Fields fieldA = new Fields( "a" );
    Fields fieldB = new Fields( 0, 1 );

    Fields appended = fieldA.appendSelector( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", "a", appended.get( 0 ) );
    assertEquals( "not equal: ", 0, appended.get( 1 ) );
    assertEquals( "not equal: ", 1, appended.get( 2 ) );
    }

  @Test
  public void testAppendSelectNamedFail()
    {
    Fields fieldA = new Fields( "a", 0 );
    Fields fieldB = new Fields( 0, 1 );

    try
      {
      Fields appended = fieldA.appendSelector( fieldB );
      fail( "did not throw exception" );
      }
    catch( Exception exception )
      {
      // do nothing
//      exception.printStackTrace();
      }
    }

  @Test
  public void testRename()
    {
    Fields fields = new Fields( "a", "b", "c", "d" );
    Fields from = new Fields( "a", "b" );
    Fields to = new Fields( "A", "B" );

    Fields renamed = fields.rename( from, to );

    assertEquals( "not equal: ", 4, renamed.size() );
    assertEquals( "not equal: ", "A", renamed.get( 0 ) );
    assertEquals( "not equal: ", "B", renamed.get( 1 ) );
    assertEquals( "not equal: ", "c", renamed.get( 2 ) );
    assertEquals( "not equal: ", "d", renamed.get( 3 ) );
    }

  @Test
  public void testRename2()
    {
    Fields fields = new Fields( "a", "b", "c", "d" );
    Fields from = new Fields( 0, 1 );
    Fields to = new Fields( "A", "B" );

    Fields renamed = fields.rename( from, to );

    assertEquals( "not equal: ", 4, renamed.size() );
    assertEquals( "not equal: ", "A", renamed.get( 0 ) );
    assertEquals( "not equal: ", "B", renamed.get( 1 ) );
    assertEquals( "not equal: ", "c", renamed.get( 2 ) );
    assertEquals( "not equal: ", "d", renamed.get( 3 ) );
    }

  @Test
  public void testRename3()
    {
    Fields fields = new Fields( "a", "b", "c", "d" );
    Fields from = new Fields( "a", "b" );
    Fields to = new Fields( 0, 1 );

    Fields renamed = fields.rename( from, to );

    assertTrue( "not isOrdered", renamed.isOrdered() );
    assertEquals( "not equal: ", 4, renamed.size() );
    assertEquals( "not equal: ", 0, renamed.get( 0 ) );
    assertEquals( "not equal: ", 1, renamed.get( 1 ) );
    assertEquals( "not equal: ", "c", renamed.get( 2 ) );
    assertEquals( "not equal: ", "d", renamed.get( 3 ) );
    }

  @Test
  public void testRename4()
    {
    Fields fields = new Fields( "a", "b", "c", "d" );
    Fields from = new Fields( "a", "b" );
    Fields to = new Fields( 3, 4 );

    Fields renamed = fields.rename( from, to );

    assertTrue( "isOrdered", !renamed.isOrdered() );
    assertEquals( "not equal: ", 4, renamed.size() );
    assertEquals( "not equal: ", 3, renamed.get( 0 ) );
    assertEquals( "not equal: ", 4, renamed.get( 1 ) );
    assertEquals( "not equal: ", "c", renamed.get( 2 ) );
    assertEquals( "not equal: ", "d", renamed.get( 3 ) );
    }

  @Test
  public void testDiff()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "a" );

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "b", diff.get( 0 ) );
    }

  @Test
  public void testDiff2()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = Fields.NONE;

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 2, diff.size() );
    assertEquals( "not equal: ", "a", diff.get( 0 ) );
    assertEquals( "not equal: ", "b", diff.get( 1 ) );
    }

//  public void testDiffDupe()
//    {
//    Fields fieldA = new Fields( "a", "b" );
//    Fields fieldB = new Fields( "a", "a" );
//
//    Fields diff = fieldA.minus( fieldB );
//
//    assertEquals( "not equal: ", 1, diff.size() );
//    assertEquals( "not equal: ", "b", diff.get( 0 ) );
//    }

  @Test
  public void testDiffSame()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "b", "a" );

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 0, diff.size() );
    }

  @Test
  public void testDiffIndex()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 0 );

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "b", diff.get( 0 ) );
    }

  @Test
  public void testDiffIndex2()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 1 );

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", "a", diff.get( 0 ) );
    }

  @Test
  public void testDiff3()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( 0, 4 );

    try
      {
      Fields diff = fieldA.subtract( fieldB );
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
  @Test
  public void testSelect()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  @Test
  public void testSelect2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( "b", "a" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  @Test
  public void testSelectPos()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields selectA = new Fields( 0, 1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    assertEquals( "not equal: ", 1, got.get( 1 ) );
    }

  /**
   * this test is reverted from original. we don't want to do the following comment
   * ~~this one is funky. regardless of the input, positions are always monotonically increasing~~
   */
  @Test
  public void testSelectPos2()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields selectA = new Fields( 1, 0 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", 1, got.get( 0 ) );
    assertEquals( "not equal: ", 0, got.get( 1 ) );
    }

  @Test
  public void testSelectMixed()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( 0, 1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  @Test
  public void testSelectMixed2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( 1, 0 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  @Test
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

  @Test
  public void testSelectMixed4()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( 2, 3, "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", 2, got.get( 0 ) );
    assertEquals( "not equal: ", 3, got.get( 1 ) );
    assertEquals( "not equal: ", "a", got.get( 2 ) );
    assertEquals( "not equal: ", "b", got.get( 3 ) );
    }

  @Test
  public void testSelectMixedNeg()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( -2, -1 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  @Test
  public void testSelectMixedNeg2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = new Fields( -1, -2 );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    assertEquals( "not equal: ", "a", got.get( 1 ) );
    }

  @Test
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

  @Test
  public void testSelectMixedNeg4()
    {
    Fields declarationA = new Fields( "a", "b", 2, 3 );
    Fields selectA = new Fields( -2, -1, "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 4, got.size() );
    assertEquals( "not equal: ", 2, got.get( 0 ) );
    assertEquals( "not equal: ", 3, got.get( 1 ) );
    assertEquals( "not equal: ", "a", got.get( 2 ) );
    assertEquals( "not equal: ", "b", got.get( 3 ) );
    }

  @Test
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

  @Test
  public void testResolveIndexOnly()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 0 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 0, got.get( 0 ) );
    }

  @Test
  public void testResolveIndexOnly2()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 1 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 1, got.get( 0 ) );
    }

  @Test
  public void testResolveIndexOnly3()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 2 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 2, got.get( 0 ) );
    }

  @Test
  public void testResolveIndexOnly4()
    {
    Fields declarationA = new Fields( 0, 1 );
    Fields declarationB = new Fields( 0, 1 );
    Fields selectA = new Fields( 3 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", 3, got.get( 0 ) );
    }

  @Test
  public void testResolveIndexAppended()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 0 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    }

  @Test
  public void testResolveIndexAppended2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 1 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    }

  @Test
  public void testResolveIndexAppended3()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 2 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "c", got.get( 0 ) );
    }

  @Test
  public void testResolveIndexAppended4()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( 3 );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "d", got.get( 0 ) );
    }

  @Test
  public void testResolveAppended()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "a" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    }

  @Test
  public void testResolveAppended2()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "b" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "b", got.get( 0 ) );
    }

  @Test
  public void testResolveAppended3()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "c" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "c", got.get( 0 ) );
    }

  @Test
  public void testResolveAppended4()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields declarationB = new Fields( "c", "d" );
    Fields selectA = new Fields( "d" );

    Fields got = Fields.resolve( selectA, declarationA, declarationB );

    assertEquals( "not equal: ", 1, got.size() );
    assertEquals( "not equal: ", "d", got.get( 0 ) );
    }

  @Test
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

  @Test
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

  @Test
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
  @Test
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

  @Test
  public void testSelectWildcard()
    {
    Fields declarationA = new Fields( "a", "b" );
    Fields selectA = Fields.ALL;

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", "a", got.get( 0 ) );
    assertEquals( "not equal: ", "b", got.get( 1 ) );
    }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testNestedFieldsFail()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "c", "d" );

    try
      {
      new Fields( fieldA, fieldB );
      fail( "did not throw exception" );
      }
    catch( IllegalArgumentException exception )
      {
      // ignore
      }
    }

  @Test
  public void testNullFieldsFail()
    {
    try
      {
      new Fields( "foo", 1, null );
      fail( "did not throw exception" );
      }
    catch( IllegalArgumentException exception )
      {
      // ignore
      }
    }

  @Test
  public void testMergeFields()
    {
    Fields fieldA = new Fields( "a", "b" );
    Fields fieldB = new Fields( "c", "d" );

    Fields join = Fields.merge( fieldA, fieldB );

    assertEquals( "not equal: ", 4, join.size() );
    assertEquals( "not equal: ", "a", join.get( 0 ) );
    assertEquals( "not equal: ", "b", join.get( 1 ) );
    assertEquals( "not equal: ", "c", join.get( 2 ) );
    assertEquals( "not equal: ", "d", join.get( 3 ) );

    fieldA = new Fields( "a", "b" );
    fieldB = new Fields( "a", "d" );

    join = Fields.merge( fieldA, fieldB );

    assertEquals( "not equal: ", 3, join.size() );
    assertEquals( "not equal: ", "a", join.get( 0 ) );
    assertEquals( "not equal: ", "b", join.get( 1 ) );
    assertEquals( "not equal: ", "d", join.get( 2 ) );

    fieldA = Fields.size( 2 );
    fieldB = new Fields( "a", "d" );

    join = Fields.merge( fieldA, fieldB );

    assertEquals( "not equal: ", 4, join.size() );
    assertEquals( "not equal: ", 0, join.get( 0 ) );
    assertEquals( "not equal: ", 1, join.get( 1 ) );
    assertEquals( "not equal: ", "a", join.get( 2 ) );
    assertEquals( "not equal: ", "d", join.get( 3 ) );
    }

  @Test
  public void testSelectFrom()
    {
    Fields declarationA = new Fields( "a", "b", "c", "d", "e", "f", "g" );
    Fields declarationB = new Fields( "A", "B", "C", "D", "E", "F", "G" );

    Fields selectA = new Fields( "b", "d", "f" );

    Fields fields = declarationA.selectPos( selectA );
    Fields results = declarationB.select( fields );

    assertEquals( "not equal: ", 3, results.size() );
    assertEquals( "not equal: ", "B", results.get( 0 ) );
    assertEquals( "not equal: ", "D", results.get( 1 ) );
    assertEquals( "not equal: ", "F", results.get( 2 ) );
    }

  @Test
  public void testTypedAppendToNone()
    {
    Fields fieldA = Fields.NONE;
    Fields fieldB = new Fields( names( "a" ), types( String.class ) );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 1, appended.size() );
    assertEquals( "not equal: ", "a", appended.get( 0 ) );

    assertEquals( "not equal: ", String.class, appended.getType( 0 ) );
    }

  @Test
  public void testTypedAppendNamed()
    {
    Fields fieldA = new Fields( names( 0, 1 ), types( int.class, int.class ) );
    Fields fieldB = new Fields( names( "a" ), types( String.class ) );

    Fields appended = fieldA.append( fieldB );

    assertEquals( "not equal: ", 3, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );
    assertEquals( "not equal: ", "a", appended.get( 2 ) );

    assertEquals( "not equal: ", int.class, appended.getType( 0 ) );
    assertEquals( "not equal: ", int.class, appended.getType( 1 ) );
    assertEquals( "not equal: ", String.class, appended.getType( 2 ) );
    }

  @Test
  public void testTypedRename()
    {
    Fields fields = new Fields( names( "a", "b", "c", "d" ), types( int.class, int.class, int.class, int.class ) );
    Fields from = new Fields( "a", "b" );
    Fields to = new Fields( names( "A", "B" ), types( String.class, String.class ) );

    Fields renamed = fields.rename( from, to );

    assertEquals( "not equal: ", 4, renamed.size() );
    assertEquals( "not equal: ", String.class, renamed.getType( 0 ) );
    assertEquals( "not equal: ", String.class, renamed.getType( 1 ) );
    assertEquals( "not equal: ", int.class, renamed.getType( 2 ) );
    assertEquals( "not equal: ", int.class, renamed.getType( 3 ) );
    }

  @Test
  public void testTypedSelect()
    {
    Fields declarationA = new Fields( names( "a", "b" ), types( int.class, String.class ) );
    Fields selectA = new Fields( "a", "b" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", int.class, got.getType( 0 ) );
    assertEquals( "not equal: ", String.class, got.getType( 1 ) );
    }

  @Test
  public void testTypedSelect2()
    {
    Fields declarationA = new Fields( names( "a", "b" ), types( int.class, String.class ) );
    Fields selectA = new Fields( "b", "a" );

    Fields got = declarationA.select( selectA );

    assertEquals( "not equal: ", 2, got.size() );
    assertEquals( "not equal: ", String.class, got.getType( 0 ) );
    assertEquals( "not equal: ", int.class, got.getType( 1 ) );
    }

  @Test
  public void testTypedDiff()
    {
    Fields fieldA = new Fields( names( "a", "b" ), types( int.class, String.class ) );
    Fields fieldB = new Fields( "a" );

    Fields diff = fieldA.subtract( fieldB );

    assertEquals( "not equal: ", 1, diff.size() );
    assertEquals( "not equal: ", String.class, diff.getType( 0 ) );
    }

  @Test
  public void testTypedApply()
    {
    Fields fieldA = new Fields( names( "a", "b" ), types( int.class, int.class ) );
    Fields fieldB = new Fields( names( "a" ), types( String.class ) );

    Fields appended = fieldA.applyTypes( fieldB );

    assertEquals( "not equal: ", 2, appended.size() );
    assertEquals( "not equal: ", "a", appended.get( 0 ) );
    assertEquals( "not equal: ", "b", appended.get( 1 ) );

    assertEquals( "not equal: ", String.class, appended.getType( 0 ) );
    assertEquals( "not equal: ", int.class, appended.getType( 1 ) );

    assertEquals( "not equal: ", 2, fieldA.size() );
    assertEquals( "not equal: ", "a", fieldA.get( 0 ) );
    assertEquals( "not equal: ", "b", fieldA.get( 1 ) );

    assertEquals( "not equal: ", int.class, fieldA.getType( 0 ) );
    assertEquals( "not equal: ", int.class, fieldA.getType( 1 ) );
    }

  @Test
  public void testApplyType()
    {
    Fields fieldA = new Fields( names( "a", "b" ), types( int.class, int.class ) );

    Fields newField = fieldA.applyType( "a", String.class );

    assertEquals( "not equal: ", 2, newField.size() );
    assertEquals( "not equal: ", "a", newField.get( 0 ) );
    assertEquals( "not equal: ", "b", newField.get( 1 ) );
    assertEquals( "not equal: ", String.class, newField.getType( 0 ) );
    assertEquals( "not equal: ", int.class, newField.getType( 1 ) );

    assertEquals( "not equal: ", 2, fieldA.size() );
    assertEquals( "not equal: ", "a", fieldA.get( 0 ) );
    assertEquals( "not equal: ", "b", fieldA.get( 1 ) );
    assertEquals( "not equal: ", int.class, fieldA.getType( 0 ) );
    assertEquals( "not equal: ", int.class, fieldA.getType( 1 ) );
    }

  @Test
  public void testFieldApply()
    {
    Fields fieldA = new Fields( String.class, Integer.TYPE );

    Fields appended = fieldA.applyFields( "a", "b" );

    assertEquals( "not equal: ", 2, appended.size() );
    assertEquals( "not equal: ", "a", appended.get( 0 ) );
    assertEquals( "not equal: ", "b", appended.get( 1 ) );

    assertEquals( "not equal: ", String.class, appended.getType( 0 ) );
    assertEquals( "not equal: ", int.class, appended.getType( 1 ) );

    appended = fieldA.applyFields( 0, 1 );

    assertEquals( "not equal: ", 2, appended.size() );
    assertEquals( "not equal: ", 0, appended.get( 0 ) );
    assertEquals( "not equal: ", 1, appended.get( 1 ) );

    assertEquals( "not equal: ", String.class, appended.getType( 0 ) );
    assertEquals( "not equal: ", int.class, appended.getType( 1 ) );
    }

  public void testMergeWithSelectors()
    {
    Fields f1 = new Fields( "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" );
    Fields f2 = new Fields( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 );
    Fields merged = Fields.merge( f1, f2 );

    assertEquals( 20, merged.size() );
    }

  public void testMergeWithSelectorsWithTypes()
    {
    Type[] types = new Type[ 10 ];
    Arrays.fill( types, int.class );

    types[ 1 ] = long.class;

    Fields f1 = new Fields( "1", "2", "3", "4", "5", "6", "7", "8", "9", "10" ).applyTypes( types );
    Fields f2 = new Fields( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ).applyTypes( types );
    Fields merged = Fields.merge( f1, f2 );

    assertEquals( 20, merged.size() );
    assertEquals( 20, merged.getTypes().length );
    assertEquals( long.class, merged.getTypes()[ 1 ] );
    assertEquals( long.class, merged.getTypes()[ 11 ] );
    }

  public void testResolveWithTypes()
    {
    Fields selector = new Fields( "ip" );
    Fields fields[] = new Fields[]{new Fields( "ip" ).applyTypes( String.class ),
                                   new Fields( "offset", "line" ).applyTypes( String.class, String.class )};

    Fields resolved = Fields.resolve( selector, fields );
    assertTrue( resolved.hasTypes() );
    }

  public void testConstructorWithNullComparableInArray()
    {
    try
      {
      new Fields( new Comparable[]{"a", null, "c"} );
      fail( "Fields constructor should have thrown an Exception" );
      }
    catch( IllegalArgumentException exception )
      {
      //expected
      }
    }

  public void testConstructorWithNullComparable()
    {
    try
      {
      Comparable comparable = null;
      new Fields( comparable );
      fail( "Fields constructor should have thrown an Exception" );
      }
    catch( IllegalArgumentException exception )
      {
      //expected
      }
    }

  public void testConstructorWithNullTypes()
    {
    try
      {
      new Fields( new Comparable[]{"a", "b", "c"}, new Type[]{int.class, null, String.class} );
      fail( "Fields constructor should have thrown an Exception" );
      }
    catch( IllegalArgumentException exception )
      {
      //expected
      }
    }

  public void testConstructorWithNullType()
    {
    try
      {
      Type type = null;
      new Fields( "a", type );
      fail( "Fields constructor should have thrown an Exception" );
      }
    catch( IllegalArgumentException exception )
      {
      //expected
      }
    }
  }
