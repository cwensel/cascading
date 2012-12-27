/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileLhs;
import static data.InputData.inputFileRhs;


public class TypedFieldedPipesPlatformTest extends PlatformTestCase
  {
  public TypedFieldedPipesPlatformTest()
    {
    }

  @Test
  public void testCoGroupIncomparableTypes() throws Exception
    {
    runJoinIncomparableTypes( false, true, false, false );
    }

  @Test
  public void testCoGroupComparableTypes() throws Exception
    {
    runJoinIncomparableTypes( false, true, true, false );
    }

  @Test
  public void testHashJoinIncomparableTypes() throws Exception
    {
    runJoinIncomparableTypes( false, false, false, false );
    }

  @Test
  public void testHashJoinComparableTypes() throws Exception
    {
    runJoinIncomparableTypes( false, false, true, false );
    }

  @Test
  public void testCoGroupIncomparableTypesDeclared() throws Exception
    {
    runJoinIncomparableTypes( false, true, false, true );
    }

  @Test
  public void testCoGroupComparableTypesDeclared() throws Exception
    {
    runJoinIncomparableTypes( false, true, true, true );
    }

  @Test
  public void testHashJoinIncomparableTypesDeclared() throws Exception
    {
    runJoinIncomparableTypes( false, false, false, true );
    }

  @Test
  public void testHashJoinComparableTypesDeclared() throws Exception
    {
    runJoinIncomparableTypes( false, false, true, true );
    }

  // merging

  @Test
  public void testGroupByIncomparableTypes() throws Exception
    {
    runJoinIncomparableTypes( true, true, false, true );
    }

  @Test
  public void testGroupByComparableTypes() throws Exception
    {
    runJoinIncomparableTypes( true, true, true, true );
    }

  /**
   * comparing streams with comparators isn't possible as Merge performs no comparison.
   * <p/>
   * so this will fail if types are not consistent
   *
   * @throws Exception
   */
  @Test
  public void testMergeIncomparableTypes() throws Exception
    {
    runJoinIncomparableTypes( true, false, false, true );
    }


  private void runJoinIncomparableTypes( boolean isMerge, boolean isGroup, boolean includeComparator, boolean isDeclared ) throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );
    getPlatform().copyFromLocal( inputFileRhs );

    Map sources = new HashMap();

    Class[] lhsTypes = new Class[]{long.class, String.class};
    Class[] rhsTypes = new Class[]{short.class, char.class};

    Fields declaredFields = isDeclared ? new Fields( "numLHS", "charLHS" ).append( new Fields( "numRHS", "charRHS" ) ) : null;

    Fields lhsFields = new Fields( "num", "char" ).applyTypes( lhsTypes );
    Fields rhsFields = new Fields( "num" + ( isDeclared ? "" : "1" ), "char" + ( isDeclared ? "" : "1" ) ).applyTypes( rhsTypes );

    sources.put( "lhs", getPlatform().getDelimitedFile( lhsFields, " ", inputFileLhs, SinkMode.KEEP ) );
    sources.put( "rhs", getPlatform().getDelimitedFile( rhsFields, " ", inputFileRhs, SinkMode.KEEP ) );

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, true, getOutputPath( getTestName() ), SinkMode.REPLACE );

    Pipe pipeLower = new Pipe( "lhs" );
    Pipe pipeUpper = new Pipe( "rhs" );

    Fields numLHS = new Fields( "num" );

    if( includeComparator )
      numLHS.setComparator( 0, Collections.reverseOrder() ); // set to disable type comparison

    Fields numRHS = new Fields( "num" + ( isDeclared ? "" : "1" ) );

    Pipe join;

    if( isMerge && isGroup )
      join = new GroupBy( Pipe.pipes( pipeLower, pipeUpper ), numLHS );
    else if( !isMerge && isGroup )
      join = new CoGroup( pipeLower, numLHS, pipeUpper, numRHS, declaredFields, new InnerJoin() );
    else if( isMerge && !isGroup )
      join = new Merge( pipeLower, pipeUpper );
    else
      join = new HashJoin( pipeLower, numLHS, pipeUpper, numRHS, declaredFields, new InnerJoin() );

    Flow flow = null;
    try
      {
      flow = getPlatform().getFlowConnector().connect( sources, sink, join );

      if( !includeComparator )
        fail( "should fail during planning" );
      }
    catch( Exception exception )
      {
      if( includeComparator )
        {
        exception.printStackTrace();
        fail( "should not fail during planning: " + exception.getMessage() );
        }
      // do nothing
      }

    if( !isMerge && includeComparator )
      {
      Class[] types = sink.getSinkFields().getTypesClasses();
      assertTrue(
        Arrays.equals(
          types,
          new Class[]{long.class, String.class, short.class, char.class}
        )
      );
      }

    }
  }