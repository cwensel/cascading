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

package cascading;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.pipe.assembly.Rename;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import org.junit.Test;

/**
 *
 */
public class TraceTest extends CascadingTestCase
  {
  @Test
  public void testOperation()
    {
    BaseOperation operation = new Identity();

    assertEqualsTrace( "cascading.TraceTest.testOperation(TraceTest.java", operation.getTrace() );
    }

  @Test
  public void testPipe()
    {
    Pipe pipe = new Pipe( "foo" );

    assertEqualsTrace( "cascading.TraceTest.testPipe(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeEach()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Each( pipe, new Fields( "a" ), new Identity() );

    assertEqualsTrace( "cascading.TraceTest.testPipeEach(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeCoGroup()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Each( pipe, new Fields( "a" ), new Identity() );

    pipe = new CoGroup( pipe, new Fields( "b" ), 4 );

    assertEqualsTrace( "cascading.TraceTest.testPipeCoGroup(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeHashJoin()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Each( pipe, new Fields( "a" ), new Identity() );
    pipe = new HashJoin( pipe, new Fields( "b" ), new Pipe( "bar" ), new Fields( "c" ) );

    assertEqualsTrace( "cascading.TraceTest.testPipeHashJoin(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeGroupBy()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Each( pipe, new Fields( "a" ), new Identity() );

    pipe = new GroupBy( pipe, new Fields( "b" ) );

    assertEqualsTrace( "cascading.TraceTest.testPipeGroupBy(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeMerge()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Each( pipe, new Fields( "a" ), new Identity() );

    pipe = new Merge( pipe, new Pipe( "bar" ) );

    assertEqualsTrace( "cascading.TraceTest.testPipeMerge(TraceTest.java", pipe.getTrace() );
    }

  @Test
  public void testPipeAssembly()
    {
    Pipe pipe = new Pipe( "foo" );

    pipe = new Rename( pipe, new Fields( "a" ), new Fields( "b" ) );

    assertEqualsTrace( "cascading.TraceTest.testPipeAssembly(TraceTest.java", pipe.getTrace() );
    }

  protected static class TestSubAssembly extends SubAssembly
    {
    public Pipe pipe;

    public TestSubAssembly()
      {
      Pipe pipe = new Pipe( "foo" );
      setPrevious( pipe );

      pipe = new Rename( pipe, new Fields( "a" ), new Fields( "b" ) );

      this.pipe = pipe;
      setTails( pipe );
      }
    }

  @Test
  public void testPipeAssemblyDeep()
    {
    TestSubAssembly pipe = new TestSubAssembly();

    assertEqualsTrace( "cascading.TraceTest.testPipeAssemblyDeep(TraceTest.java", pipe.getTrace() );
    assertEqualsTrace( "cascading.TraceTest$TestSubAssembly.<init>(TraceTest.java", pipe.pipe.getTrace() );
    assertEqualsTrace( "cascading.TraceTest$TestSubAssembly.<init>(TraceTest.java", pipe.getTails()[ 0 ].getTrace() );
    }

  @Test
  public void testTap()
    {
    Tap tap = new Tap()
    {
    @Override
    public String getIdentifier()
      {
      return null;
      }

    @Override
    public TupleEntryIterator openForRead( FlowProcess flowProcess, Object object ) throws IOException
      {
      return null;
      }

    @Override
    public TupleEntryCollector openForWrite( FlowProcess flowProcess, Object object ) throws IOException
      {
      return null;
      }

    @Override
    public boolean createResource( Object conf ) throws IOException
      {
      return false;
      }

    @Override
    public boolean deleteResource( Object conf ) throws IOException
      {
      return false;
      }

    @Override
    public boolean resourceExists( Object conf ) throws IOException
      {
      return false;
      }

    @Override
    public long getModifiedTime( Object conf ) throws IOException
      {
      return 0;
      }
    };

    assertEqualsTrace( "cascading.TraceTest.testTap(TraceTest.java", tap.getTrace() );
    }

  @Test
  public void testScheme()
    {
    Scheme scheme = new Scheme()
    {
    @Override
    public void sourceConfInit( FlowProcess flowProcess, Tap tap, Object conf )
      {
      }

    @Override
    public void sinkConfInit( FlowProcess flowProcess, Tap tap, Object conf )
      {
      }

    @Override
    public boolean source( FlowProcess flowProcess, SourceCall sourceCall ) throws IOException
      {
      return false;
      }

    @Override
    public void sink( FlowProcess flowProcess, SinkCall sinkCall ) throws IOException
      {
      }
    };

    assertEqualsTrace( "cascading.TraceTest.testScheme(TraceTest.java", scheme.getTrace() );
    }

  public static void assertEqualsTrace( String expected, String trace )
    {
    String substring = trace.substring( 0, trace.lastIndexOf( ":" ) );
    assertEquals( expected, substring );
    }
  }
