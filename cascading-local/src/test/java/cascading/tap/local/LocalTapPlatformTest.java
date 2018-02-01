/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.local;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.local.Compressors;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import data.InputData;
import org.junit.Test;

import static data.InputData.inputFileNums20;

/**
 *
 */
public class LocalTapPlatformTest extends PlatformTestCase implements Serializable
  {
  @Test
  public void testIO()
    {
    String lines = "line1\nline2\n";
    System.setIn( new ByteArrayInputStream( lines.getBytes() ) );
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    System.setOut( new PrintStream( output ) );

    Tap source = new StdInTap( new TextLine( new Fields( "line" ) ) );
    Tap sink = new StdOutTap( new TextLine( new Fields( "line" ) ) );

    Pipe pipe = new Pipe( "io" );

    Flow flow = new LocalFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertEquals( lines, output.toString() );
    }

  /** Extension of TextLine that actually sets properties. */
  private static class SchemeWithProperties extends TextLine
    {
    public SchemeWithProperties( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public void sourceConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
      {
      if( !"connector-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      conf.setProperty( "replace", "source-replace" );
      conf.setProperty( "local", "source-local" );
      super.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sinkConfInit( FlowProcess<? extends Properties> flowProcess, Tap<Properties, InputStream, OutputStream> tap, Properties conf )
      {
      if( !"connector-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      conf.setProperty( "replace", "sink-replace" );
      conf.setProperty( "local", "sink-local" );
      super.sinkConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
      {
      if( !"connector-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"source-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );

      if( !"source-local".equals( flowProcess.getProperty( "local" ) ) )
        throw new RuntimeException( "not local value" );

      super.sourcePrepare( flowProcess, sourceCall );
      }

    @Override
    public void sinkPrepare( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
      {
      if( !"connector-default".equals( flowProcess.getProperty( "default" ) ) )
        throw new RuntimeException( "not default value" );

      if( !"sink-replace".equals( flowProcess.getProperty( "replace" ) ) )
        throw new RuntimeException( "not replaced value" );

      if( !"sink-local".equals( flowProcess.getProperty( "local" ) ) )
        throw new RuntimeException( "not local value" );

      super.sinkPrepare( flowProcess, sinkCall );
      }
    }

  @Test
  public void testSourceConfInit() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Scheme scheme = new SchemeWithProperties( new Fields( "line" ) );
    Tap source = getPlatform().getTap( scheme, inputFileNums20, SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    Tap sink = getPlatform().getTextFile( getOutputPath( "sourceconfinit" ), SinkMode.REPLACE );

    Properties properties = new Properties();
    properties.setProperty( "default", "connector-default" );
    properties.setProperty( "replace", "connector-replace" );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }

  @Test
  public void testSinkConfInit() throws IOException
    {
    getPlatform().copyFromLocal( inputFileNums20 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileNums20, SinkMode.KEEP );

    Pipe pipe = new Pipe( "test" );

    Scheme scheme = new SchemeWithProperties( new Fields( "line" ) );
    Tap sink = getPlatform().getTap( scheme, getOutputPath( "sinkconfinit" ), SinkMode.REPLACE );

    Properties properties = new Properties();
    properties.setProperty( "default", "connector-default" );
    properties.setProperty( "replace", "connector-replace" );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    assertTrue( flow.resourceExists( sink ) );
    }

  @Test
  public void testDirTap() throws Exception
    {
    Tap source = new DirTap( new TextLine(), InputData.inputPath, "glob:**/*.txt" );
    DirTap sink = new DirTap( new TextLine(), getOutputPath(), SinkMode.REPLACE );
    Pipe pipe = new Pipe( "copy" );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    List<Tuple> list = getSinkAsList( flow );

    assertEquals( 674, list.size() );
    }

  @Test
  public void testSchemeCompression() throws Exception
    {
    Tap source = new DirTap( new TextLine(), InputData.inputPath, "glob:**/*.txt" );
    DirTap compressed = new DirTap( new TextLine( Compressors.GZIP ), getOutputPath( "compressed" ), SinkMode.REPLACE );
    DirTap sink = new DirTap( new TextLine(), getOutputPath( "uncompressed" ), SinkMode.REPLACE );

    Flow first = getPlatform().getFlowConnector().connect( "first", source, compressed, new Pipe( "copy" ) );

    first.complete();

    Flow second = getPlatform().getFlowConnector().connect( "second", compressed, sink, new Pipe( "copy" ) );

    second.complete();

    List<Tuple> list = getSinkAsList( second );

    assertEquals( 674, list.size() );
    }
  }
