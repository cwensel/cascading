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

package cascading.pipe;

import cascading.CascadingTestCase;
import cascading.TestFunction;
import cascading.operation.Identity;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

public class PipeParamTest extends CascadingTestCase
  {
  Fields[] ARGS_PASS = new Fields[]{new Fields( "x" ), Fields.ALL, Fields.GROUP, Fields.VALUES};
  Fields[] FUNCS_PASS = new Fields[]{new Fields( "y" ), Fields.UNKNOWN, Fields.ALL, Fields.GROUP, Fields.VALUES,
                                     Fields.ARGS};
  Fields[] OUTS_PASS = new Fields[]{new Fields( "y" ), Fields.RESULTS, Fields.ALL};

  Fields[] ARGS_FAIL = new Fields[]{Fields.UNKNOWN, Fields.ARGS, Fields.RESULTS};
  Fields[] FUNCS_FAIL = new Fields[]{Fields.RESULTS};
  Fields[] OUTS_FAIL = new Fields[]{Fields.UNKNOWN, Fields.ARGS, Fields.GROUP, Fields.VALUES};

  public PipeParamTest()
    {
    super();
    }

  @Test
  public void testEachPassCtor()
    {
    for( Fields arg : ARGS_PASS )
      {
      for( Fields func : FUNCS_PASS )
        {
        for( Fields out : OUTS_PASS )
          {
          try
            {
            new Each( new Pipe( "test" ), arg, new TestFunction( func, new Tuple( "value" ) ), out );
            }
          catch( Exception exception )
            {
            fail( "failed on: " + arg.print() + " " + func.print() + " " + out.print() );
            }
          }
        }
      }
    }

  @Test
  public void testEachFailCtor()
    {
    for( Fields arg : ARGS_FAIL )
      {
      for( Fields func : FUNCS_PASS )
        {
        for( Fields out : OUTS_PASS )
          {
          try
            {
            new Each( new Pipe( "test" ), arg, new TestFunction( func, new Tuple( "value" ) ), out );
            fail( "failed on: " + arg.print() + " " + func.print() + " " + out.print() );
            }
          catch( Exception exception )
            {
            }
          }
        }
      }
    for( Fields arg : ARGS_PASS )
      {
      for( Fields func : FUNCS_FAIL )
        {
        for( Fields out : OUTS_PASS )
          {
          try
            {
            new Each( new Pipe( "test" ), arg, new TestFunction( func, new Tuple( "value" ) ), out );
            fail( "failed on: " + arg.print() + " " + func.print() + " " + out.print() );
            }
          catch( Exception exception )
            {
            }
          }
        }
      }
    for( Fields arg : ARGS_PASS )
      {
      for( Fields func : FUNCS_PASS )
        {
        for( Fields out : OUTS_FAIL )
          {
          try
            {
            new Each( new Pipe( "test" ), arg, new TestFunction( func, new Tuple( "value" ) ), out );
            fail( "failed on: " + arg.print() + " " + func.print() + " " + out.print() );
            }
          catch( Exception exception )
            {
            }
          }
        }
      }
    }

  @Test
  public void testGetFirst()
    {
    Pipe pipeFirst = new Pipe( "first" );
    Pipe pipe = new Pipe( pipeFirst );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );

    assertEquals( pipeFirst, pipe.getHeads()[ 0 ] );
    }

  @Test
  public void testGetFirstSplit()
    {
    Pipe pipeFirst = new Pipe( "first" );
    Pipe pipe = new Pipe( pipeFirst );
    Pipe pipeA = new Pipe( pipe );
    Pipe pipeB = new Pipe( pipe );
    pipeA = new Pipe( pipeA );
    pipeB = new Pipe( pipeB );

    assertEquals( pipeFirst, pipeA.getHeads()[ 0 ] );
    assertEquals( pipeFirst, pipeB.getHeads()[ 0 ] );
    }

  @Test
  public void testGetFirstJoin()
    {
    Pipe pipeFirst = new Pipe( "first" );
    Pipe pipeSecond = new Pipe( "second" );
    Pipe pipe = new CoGroup( pipeFirst, pipeSecond );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );

    assertTrue( pipe.getHeads()[ 0 ] == pipeFirst || pipe.getHeads()[ 0 ] == pipeSecond );
    }

  private static class NestedSubAssembly extends SubAssembly
    {
    private NestedSubAssembly( Pipe pipe )
      {
      Pipe pipe1 = new Pipe( "second", pipe );

      Pipe pipe2 = new Pipe( "third", pipe );
      pipe2 = new Each( pipe2, new Identity() );
      pipe2 = new Pipe( "fourth", pipe2 );

      setTails( pipe1, pipe2 );
      }
    }

  @Test
  public void testGetNames()
    {
    Pipe pipe = new Pipe( "first" );
    pipe = new NestedSubAssembly( pipe );
    Pipe pipe1 = new Pipe( "fifth", ( (SubAssembly) pipe ).getTails()[ 0 ] );
    Pipe pipe2 = new Pipe( "sixth", ( (SubAssembly) pipe ).getTails()[ 1 ] );

    assertEquals( 6, Pipe.names( pipe1, pipe2 ).length );
    assertEquals( 1, Pipe.named( "second", pipe1, pipe2 ).length );
    assertEquals( 1, Pipe.named( "sixth", pipe1, pipe2 ).length );
    assertEquals( pipe2, Pipe.named( "sixth", pipe1, pipe2 )[ 0 ] );
    }
  }
