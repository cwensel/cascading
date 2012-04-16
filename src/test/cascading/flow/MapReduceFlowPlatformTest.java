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

package cascading.flow;

import java.io.IOException;
import java.net.URI;

import cascading.PlatformTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.MapReduceFlow;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.test.HadoopPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 *
 */
@PlatformRunner.Platform({HadoopPlatform.class})
public class MapReduceFlowPlatformTest extends PlatformTestCase
  {

  public MapReduceFlowPlatformTest()
    {
    super( true );
    }

  @Test
  public void testFlow() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    JobConf conf = new JobConf( ( (HadoopPlatform) getPlatform() ).getJobConf() );
    conf.setJobName( "mrflow" );

    conf.setOutputKeyClass( LongWritable.class );
    conf.setOutputValueClass( Text.class );

    conf.setMapperClass( IdentityMapper.class );
    conf.setReducerClass( IdentityReducer.class );

    conf.setInputFormat( TextInputFormat.class );
    conf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( conf, new Path( inputFileApache ) );

    String outputPath = getOutputPath( "flowTest" );
    FileOutputFormat.setOutputPath( conf, new Path( outputPath ) );

    Flow flow = new MapReduceFlow( "mrflow", conf, true );

    validateLength( flow.openTapForRead( new Hfs( new TextLine(), inputFileApache ) ), 10 );

    flow.complete();

    validateLength( flow.openTapForRead( new Hfs( new TextLine(), outputPath ) ), 10 );
    }

  private String remove( String path, boolean delete ) throws IOException
    {
    FileSystem fs = FileSystem.get( URI.create( path ), HadoopPlanner.createJobConf( getProperties() ) );

    if( delete )
      fs.delete( new Path( path ), true );

    return path;
    }

  @Test
  public void testCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    // Setup two standard cascading flows that will generate the input for the first MapReduceFlow
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( inputFileApache, false ) );
    String sinkPath4 = getOutputPath( "flow4" );
    Tap sink1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( sinkPath4, true ), true );
    Flow firstFlow = new HadoopFlowConnector( getProperties() ).connect( source1, sink1, new Pipe( "first-flow" ) );

    String sinkPath5 = getOutputPath( "flow5" );
    Tap sink2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( sinkPath5, true ), true );
    Flow secondFlow = new HadoopFlowConnector( getProperties() ).connect( sink1, sink2, new Pipe( "second-flow" ) );

    JobConf defaultConf = HadoopPlanner.createJobConf( getProperties() );

    JobConf firstConf = new JobConf( defaultConf );
    firstConf.setJobName( "first-mr" );

    firstConf.setOutputKeyClass( LongWritable.class );
    firstConf.setOutputValueClass( Text.class );

    firstConf.setMapperClass( IdentityMapper.class );
    firstConf.setReducerClass( IdentityReducer.class );

    firstConf.setInputFormat( TextInputFormat.class );
    firstConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( firstConf, new Path( remove( sinkPath5, true ) ) );
    String sinkPath1 = getOutputPath( "flow1" );
    FileOutputFormat.setOutputPath( firstConf, new Path( remove( sinkPath1, true ) ) );

    Flow firstMR = new MapReduceFlow( firstConf, true );

    JobConf secondConf = new JobConf( defaultConf );
    secondConf.setJobName( "second-mr" );

    secondConf.setOutputKeyClass( LongWritable.class );
    secondConf.setOutputValueClass( Text.class );

    secondConf.setMapperClass( IdentityMapper.class );
    secondConf.setReducerClass( IdentityReducer.class );

    secondConf.setInputFormat( TextInputFormat.class );
    secondConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( secondConf, new Path( remove( sinkPath1, true ) ) );
    String sinkPath2 = getOutputPath( "flow2" );
    FileOutputFormat.setOutputPath( secondConf, new Path( remove( sinkPath2, true ) ) );

    Flow secondMR = new MapReduceFlow( secondConf, true );

    JobConf thirdConf = new JobConf( defaultConf );
    thirdConf.setJobName( "third-mr" );

    thirdConf.setOutputKeyClass( LongWritable.class );
    thirdConf.setOutputValueClass( Text.class );

    thirdConf.setMapperClass( IdentityMapper.class );
    thirdConf.setReducerClass( IdentityReducer.class );

    thirdConf.setInputFormat( TextInputFormat.class );
    thirdConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( thirdConf, new Path( remove( sinkPath2, true ) ) );
    String sinkPath3 = getOutputPath( "flow3" );
    FileOutputFormat.setOutputPath( thirdConf, new Path( remove( sinkPath3, true ) ) );

    Flow thirdMR = new MapReduceFlow( thirdConf, true );

    CascadeConnector cascadeConnector = new CascadeConnector();

    // pass out of order
    Cascade cascade = cascadeConnector.connect( firstFlow, secondFlow, thirdMR, firstMR, secondMR );

    cascade.complete();

    validateLength( thirdMR.openTapForRead( new Hfs( new TextLine(), sinkPath3 ) ), 10 );
    }
  }
