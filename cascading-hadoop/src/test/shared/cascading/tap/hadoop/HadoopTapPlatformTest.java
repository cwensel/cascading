/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.util.Hadoop18TapUtil;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import data.InputData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static data.InputData.*;

/**
 *
 */
public class HadoopTapPlatformTest extends PlatformTestCase implements Serializable
  {
  private static final Logger LOG = LoggerFactory.getLogger( HadoopTapPlatformTest.class );

  public HadoopTapPlatformTest()
    {
    super( true );
    }

  @Test
  public void testDfs() throws URISyntaxException, IOException
    {
    if( !getPlatform().isUseCluster() )
      return;

    // Dfs only runs on hdfs://, not just any distributed filesystem. if unavailable, skip test
    if( !( (BaseHadoopPlatform) getPlatform() ).isHDFSAvailable() )
      {
      LOG.warn( "skipped Dfs tests, HDFS is unavailable on current platform" );
      return;
      }

    Tap tap = new Dfs( new SequenceFile( new Fields( "foo" ) ), "some/path" );

    String path = tap.getFullIdentifier( getPlatform().getFlowProcess() );
    assertFalse( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "file" ) );

    new Dfs( new SequenceFile( new Fields( "foo" ) ), "hdfs://localhost:5001/some/path" );
    new Dfs( new SequenceFile( new Fields( "foo" ) ), new URI( "hdfs://localhost:5001/some/path" ) );

    try
      {
      new Dfs( new SequenceFile( new Fields( "foo" ) ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }

    try
      {
      new Dfs( new SequenceFile( new Fields( "foo" ) ), new URI( "s3://localhost:5001/some/path" ) );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  @Test
  public void testLfs() throws URISyntaxException, IOException
    {
    Tap tap = new Lfs( new SequenceFile( new Fields( "foo" ) ), "some/path" );

    String path = tap.getFullIdentifier( getPlatform().getFlowProcess() );
    assertTrue( "wrong scheme", new Path( path ).toUri().getScheme().equalsIgnoreCase( "file" ) );

    new Lfs( new SequenceFile( new Fields( "foo" ) ), "file:///some/path" );

    try
      {
      new Lfs( new SequenceFile( new Fields( "foo" ) ), "s3://localhost:5001/some/path" );
      fail( "not valid url" );
      }
    catch( Exception exception )
      {
      }
    }

  public class CommentScheme extends TextLine
    {
    public CommentScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public boolean source( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall ) throws IOException
      {
      boolean success = sourceCall.getInput().next( sourceCall.getContext()[ 0 ], sourceCall.getContext()[ 1 ] );

      if( !success )
        return false;

      if( sourceCall.getContext()[ 1 ].toString().matches( "^\\s*#.*$" ) )
        return source( flowProcess, sourceCall );

      sourceHandleInput( sourceCall );

      return true;
      }
    }

  @Test
  public void testNullsFromScheme() throws IOException
    {
    getPlatform().copyFromLocal( inputFileComments );

    Tap source = new Hfs( new CommentScheme( new Fields( "line" ) ), inputFileComments );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Hfs( new TextLine( 1 ), getOutputPath( "testnulls" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    TupleEntryIterator iterator = flow.openSink();

    assertEquals( "not equal: tuple.get(1)", "1 a", iterator.next().getObject( 1 ) );

    iterator.close();

    // confirm the tuple iterator can handle nulls from the source
    validateLength( flow.openSource(), 5 );
    }

  public class ResolvedScheme extends TextLine
    {
    private final Fields expectedFields;

    public ResolvedScheme( Fields expectedFields )
      {
      this.expectedFields = expectedFields;
      }

    @Override
    public void sinkPrepare( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
      {
      Fields found = sinkCall.getOutgoingEntry().getFields();

      if( !found.equals( expectedFields ) )
        throw new RuntimeException( "fields to not match, expect: " + expectedFields + ", found: " + found );

      super.sinkPrepare( flowProcess, sinkCall );
      }

    @Override
    public void sink( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object[], OutputCollector> sinkCall ) throws IOException
      {
      Fields found = sinkCall.getOutgoingEntry().getFields();

      if( !found.equals( expectedFields ) )
        throw new RuntimeException( "fields to not match, expect: " + expectedFields + ", found: " + found );

      super.sink( flowProcess, sinkCall );
      }
    }

  @Test
  public void testResolvedSinkFields() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = new Hfs( new TextLine( new Fields( "line" ) ), inputFileLower );

    Pipe pipe = new Pipe( "test" );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );
    pipe = new Each( pipe, new Fields( "line" ), splitter );

    Tap sink = new Hfs( new ResolvedScheme( new Fields( "num", "char" ) ), getOutputPath( "resolvedfields" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector( getProperties() ).connect( source, sink, pipe );

    flow.complete();

    List<Tuple> tuples = asList( flow, sink );
    List<Object> values = new ArrayList<Object>();
    for( Tuple tuple : tuples )
      values.add( tuple.getObject( 1 ) );

    assertTrue( values.contains( "1\ta" ) );
    assertTrue( values.contains( "2\tb" ) );
    assertTrue( values.contains( "3\tc" ) );
    assertTrue( values.contains( "4\td" ) );
    assertTrue( values.contains( "5\te" ) );

    assertEquals( 5, tuples.size() );

    // confirm the tuple iterator can handle nulls from the source
    assertEquals( 5, asList( flow, source ).size() );
    }

  @Test
  public void testGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r],owe?}.txt" );

    assertEquals( 2, source.getTaps().length );

    // show globhfs will just match a directory if ended with a /
    assertEquals( 1, new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "../?ata/" ).getTaps().length );

    Tap sink = new Hfs( new TextLine(), getOutputPath( "glob" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = getPlatform().getFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "glob2" ), SinkMode.REPLACE );

    Flow nextFlow = getPlatform().getFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  @Test
  public void testNestedMultiSourceGlobHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    assertEquals( 2, source.getNumChildTaps() );

    // using null pos so all fields are written
    Tap sink = new Hfs( new TextLine(), getOutputPath( "globmultisource" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), "\\s" );
    Pipe concatPipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Flow concatFlow = getPlatform().getFlowConnector( getProperties() ).connect( "first", source, sink, concatPipe );

    Tap nextSink = new Hfs( new TextLine(), getOutputPath( "globmultiource2" ), SinkMode.REPLACE );

    Flow nextFlow = getPlatform().getFlowConnector( getProperties() ).connect( "second", sink, nextSink, concatPipe );

    Cascade cascade = new CascadeConnector().connect( concatFlow, nextFlow );

    cascade.complete();

    validateLength( concatFlow, 10 );
    }

  @Test
  public void testMultiSourceIterator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    GlobHfs source1 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r]}.txt" );
    GlobHfs source2 = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{owe?}.txt" );

    MultiSourceTap source = new MultiSourceTap( source1, source2 );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10 );

    GlobHfs sourceMulti = new GlobHfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "?{ppe[_r],owe?}.txt" );

    source = new MultiSourceTap( sourceMulti );

    validateLength( source.openForRead( getPlatform().getFlowProcess() ), 10, null );
    }

  @Test
  public void testCommitResource() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    final int[] count = {0};
    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "committap" ), SinkMode.REPLACE )
    {
    @Override
    public boolean commitResource( Configuration conf ) throws IOException
      {
      count[ 0 ] = count[ 0 ] + 1;
      return true;
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertEquals( 1, count[ 0 ] );
    validateLength( flow, 8, null );
    }

  @Test
  public void testCommitResourceFails() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "committapfail" ), SinkMode.REPLACE )
    {
    @Override
    public boolean commitResource( Configuration conf ) throws IOException
      {
      throw new IOException( "failed intentionally" );
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // success
      }
    }

  @Test
  public void testHfsAsterisk() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "*" );

    assertTrue( sourceExists.resourceExists( getPlatform().getFlowProcess() ) );

    TupleEntryIterator iterator = sourceExists.openForRead( getPlatform().getFlowProcess() );
    assertTrue( iterator.hasNext() );
    iterator.close();

    try
      {
      Hfs sourceNotExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "/blah/" );
      iterator = sourceNotExists.openForRead( getPlatform().getFlowProcess() );
      fail();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }

  @Test
  public void testHfsBracketAsterisk() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "{*}" );

    assertTrue( sourceExists.resourceExists( getPlatform().getFlowProcess() ) );

    TupleEntryIterator iterator = sourceExists.openForRead( getPlatform().getFlowProcess() );
    assertTrue( iterator.hasNext() );
    iterator.close();

    try
      {
      Hfs sourceNotExists = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputPath + "/blah/" );
      iterator = sourceNotExists.openForRead( getPlatform().getFlowProcess() );
      fail();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }

  public class DupeConfigScheme extends TextLine
    {
    public DupeConfigScheme( Fields sourceFields )
      {
      super( sourceFields );
      }

    @Override
    public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf )
      {
      if( conf.get( "this.is.a.dupe" ) != null )
        throw new IllegalStateException( "has dupe config value" );

      conf.set( "this.is.a.dupe", "dupe" );

      super.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object[], RecordReader> sourceCall )
      {
      if( flowProcess.getStringProperty( "this.is.a.dupe" ) == null )
        throw new IllegalStateException( "has no dupe config value" );

      super.sourcePrepare( flowProcess, sourceCall );
      }
    }

  @Test
  public void testDupeConfigFromScheme() throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTap( new DupeConfigScheme( new Fields( "offset", "line" ) ), inputFileUpper, SinkMode.KEEP );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "dupeconfig" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testMissingInputFormat() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextDelimited( new Fields( "offset", "line" ) ), inputFileApache )
    {
    @Override
    public void sourceConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
      {
      // don't set input format
      //super.sourceConfInit( process, conf );
      }
    };

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "missinginputformat" ), SinkMode.REPLACE );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );
      flow.complete();
      fail( "did not test for missing input format" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testChildIdentifiers() throws Exception
    {
    if( !getPlatform().isUseCluster() )
      return;

    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Configuration jobConf = ( (BaseHadoopPlatform) getPlatform() ).getConfiguration();

    Hfs tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( "multifiles" ) );

    tap.deleteResource( getPlatform().getFlowProcess() );

    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "missing", 0, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap.createResource( getPlatform().getFlowProcess() );

    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "no children", 0, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "no children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "no children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    writeFileTo( "multifiles/A" );
    writeFileTo( "multifiles/B" );

    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "/" );

    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "root", -1, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "root", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "root", 1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), "./" );

    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "current", -1, tap.getChildIdentifiers( jobConf, 0, false ) );

    tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( "hiddenfiles" ) );

    writeFileTo( "hiddenfiles/A" );
    writeFileTo( "hiddenfiles/B" );
    writeFileTo( "hiddenfiles/.hidden" );

    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 2, false ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, true ) );
    assertEqualsSize( "children", 2, tap.getChildIdentifiers( jobConf, 1, false ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, true ) );
    assertEqualsSize( "children", 1, tap.getChildIdentifiers( jobConf, 0, false ) );
    }

  public void assertEqualsSize( String message, int expected, String[] actual )
    {
//    System.out.println( message + ": " + Arrays.toString( actual ) );

    if( expected == -1 )
      return;

    assertEquals( expected, actual.length );
    }

  private void writeFileTo( String path ) throws IOException
    {
    Hfs tap = new Hfs( new TextLine( new Fields( "offset", "line" ) ), getOutputPath( path ) );

    TupleEntryCollector collector = tap.openForWrite( getPlatform().getFlowProcess() );

    collector.add( new Tuple( 1, "1" ) );

    collector.close();
    }

  @Test
  public void testPrepareResource() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    final int[] readCount = {0};
    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache )
    {
    @Override
    public boolean prepareResourceForRead( Configuration conf ) throws IOException
      {
      readCount[ 0 ] = readCount[ 0 ] + 1;
      return true;
      }
    };

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    final int[] writeCount = {0};
    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "preparetap" ), SinkMode.REPLACE )
    {
    @Override
    public boolean prepareResourceForWrite( Configuration conf ) throws IOException
      {
      writeCount[ 0 ] = writeCount[ 0 ] + 1;
      return true;
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    assertEquals( 1, readCount[ 0 ] );
    assertEquals( 1, writeCount[ 0 ] );
    validateLength( flow, 8, null );
    }

  @Test
  public void testPrepareResourceForReadFails() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache )
    {
    @Override
    public boolean prepareResourceForRead( Configuration conf ) throws IOException
      {
      throw new IOException( "failed intentionally" );
      }
    };

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "preparereadtapfail" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // success
      }
    }

  @Test
  public void testPrepareResourceForWriteFails() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = new Hfs( new TextDelimited( Fields.ALL ), getOutputPath( "preparewritetapfail" ), SinkMode.REPLACE )
    {
    @Override
    public boolean prepareResourceForWrite( Configuration conf ) throws IOException
      {
      throw new IOException( "failed intentionally" );
      }
    };

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // success
      }
    }

  @Test
  public void testTemporarySinkPathIsDeleted() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap source = getPlatform().getDelimitedFile( new Fields( "a", "b" ), " ", inputFileLowerOffset );

    Pipe pipe = new Pipe( "test" );

    String outputPath = getOutputPath( "partition-tap-sink" );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "a" ), " ", outputPath );
    sink = getPlatform().getPartitionTap( sink, new DelimitedPartition( new Fields( "b" ) ), 1 );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );
    flow.complete();
    
    Path tempPath = new Path( outputPath, Hadoop18TapUtil.TEMPORARY_PATH );
    FileSystem fileSystem = tempPath.getFileSystem( ( Configuration ) flow.getConfigCopy() );
    
    assertFalse( fileSystem.exists( tempPath ) );
    }

  }
