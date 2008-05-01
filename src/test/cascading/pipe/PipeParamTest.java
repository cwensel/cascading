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

package cascading.pipe;

import cascading.CascadingTestCase;
import cascading.TestFunction;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/** @version : IntelliJGuide,v 1.13 2001/03/22 22:35:22 SYSTEM Exp $ */
public class PipeParamTest extends CascadingTestCase
  {
  Fields[] ARGS_PASS = new Fields[]{new Fields( "x" ), Fields.ALL, Fields.KEYS, Fields.VALUES};
  Fields[] FUNCS_PASS = new Fields[]{new Fields( "y" ), Fields.UNKNOWN, Fields.ALL, Fields.KEYS, Fields.VALUES, Fields.ARGS};
  Fields[] OUTS_PASS = new Fields[]{new Fields( "y" ), Fields.RESULTS, Fields.ALL};

  Fields[] ARGS_FAIL = new Fields[]{Fields.UNKNOWN, Fields.ARGS, Fields.RESULTS};
  Fields[] FUNCS_FAIL = new Fields[]{Fields.RESULTS};
  Fields[] OUTS_FAIL = new Fields[]{Fields.UNKNOWN, Fields.ARGS, Fields.KEYS, Fields.VALUES};

  public PipeParamTest()
    {
    super( "pipe parameters test" );
    }

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

  public void testGetFirst()
    {
    Pipe pipeFirst = new Pipe( "first" );
    Pipe pipe = new Pipe( pipeFirst );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );

    assertEquals( pipeFirst, pipe.getHeads()[ 0 ] );
    }

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

  public void testGetFirstJoin()
    {
    Pipe pipeFirst = new Pipe( "first" );
    Pipe pipeSecond = new Pipe( "second" );
    Pipe pipe = new Group( pipeFirst, pipeSecond );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );
    pipe = new Pipe( pipe );

    assertTrue( pipe.getHeads()[ 0 ] == pipeFirst || pipe.getHeads()[ 0 ] == pipeSecond );
    }
  }
