/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import cascading.ClusterTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
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

/**
 *
 */
public class MapReduceFlowTest extends ClusterTestCase
  {
  String inputFileApache = "build/test/data/apache.10.txt";
  String outputPath1 = "build/test/output/mrflow/flow1/";
  String outputPath2 = "build/test/output/mrflow/flow2/";
  String outputPath3 = "build/test/output/mrflow/flow3/";
  String outputPath4 = "build/test/output/mrflow/flow4/";
  String outputPath5 = "build/test/output/mrflow/flow5/";

  public MapReduceFlowTest()
    {
    super( "map-reduce flow test", true );
    }

  public void testFlow() throws IOException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    JobConf defaultConf = MultiMapReducePlanner.getJobConf( getProperties() );

    JobConf conf = new JobConf( defaultConf );
    conf.setJobName( "mrflow" );

    conf.setOutputKeyClass( LongWritable.class );
    conf.setOutputValueClass( Text.class );

    conf.setMapperClass( IdentityMapper.class );
    conf.setReducerClass( IdentityReducer.class );

    conf.setInputFormat( TextInputFormat.class );
    conf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( conf, new Path( inputFileApache ) );
    FileOutputFormat.setOutputPath( conf, new Path( outputPath1 ) );

    Flow flow = new MapReduceFlow( "mrflow", conf, true );

    validateLength( flow.openSource(), 10 );

    flow.complete();

    validateLength( flow.openSink(), 10 );
    }

  private String remove( String path, boolean delete ) throws IOException
    {
    FileSystem fs = FileSystem.get( URI.create( path ), MultiMapReducePlanner.getJobConf( getProperties() ) );

    if( delete )
      fs.delete( new Path( path ), true );

    return path;
    }

  public void testCascade() throws IOException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    // Setup two standard cascading flows that will generate the input for the first MapReduceFlow
    Tap source1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( inputFileApache, false ) );
    Tap sink1 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( outputPath4, true ), true );
    Flow firstFlow = new FlowConnector( getProperties() ).connect( source1, sink1, new Pipe( "first-flow" ) );

    Tap sink2 = new Hfs( new TextLine( new Fields( "offset", "line" ) ), remove( outputPath5, true ), true );
    Flow secondFlow = new FlowConnector( getProperties() ).connect( sink1, sink2, new Pipe( "second-flow" ) );

    JobConf defaultConf = MultiMapReducePlanner.getJobConf( getProperties() );

    JobConf firstConf = new JobConf( defaultConf );
    firstConf.setJobName( "first-mr" );

    firstConf.setOutputKeyClass( LongWritable.class );
    firstConf.setOutputValueClass( Text.class );

    firstConf.setMapperClass( IdentityMapper.class );
    firstConf.setReducerClass( IdentityReducer.class );

    firstConf.setInputFormat( TextInputFormat.class );
    firstConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( firstConf, new Path( remove( outputPath5, true ) ) );
    FileOutputFormat.setOutputPath( firstConf, new Path( remove( outputPath1, true ) ) );

    Flow firstMR = new MapReduceFlow( firstConf, true );

    JobConf secondConf = new JobConf( defaultConf );
    secondConf.setJobName( "second-mr" );

    secondConf.setOutputKeyClass( LongWritable.class );
    secondConf.setOutputValueClass( Text.class );

    secondConf.setMapperClass( IdentityMapper.class );
    secondConf.setReducerClass( IdentityReducer.class );

    secondConf.setInputFormat( TextInputFormat.class );
    secondConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( secondConf, new Path( remove( outputPath1, true ) ) );
    FileOutputFormat.setOutputPath( secondConf, new Path( remove( outputPath2, true ) ) );

    Flow secondMR = new MapReduceFlow( secondConf, true );

    JobConf thirdConf = new JobConf( defaultConf );
    thirdConf.setJobName( "third-mr" );

    thirdConf.setOutputKeyClass( LongWritable.class );
    thirdConf.setOutputValueClass( Text.class );

    thirdConf.setMapperClass( IdentityMapper.class );
    thirdConf.setReducerClass( IdentityReducer.class );

    thirdConf.setInputFormat( TextInputFormat.class );
    thirdConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( thirdConf, new Path( remove( outputPath2, true ) ) );
    FileOutputFormat.setOutputPath( thirdConf, new Path( remove( outputPath3, true ) ) );

    Flow thirdMR = new MapReduceFlow( thirdConf, true );

    CascadeConnector cascadeConnector = new CascadeConnector();

    // pass out of order
    Cascade cascade = cascadeConnector.connect( firstFlow, secondFlow, thirdMR, firstMR, secondMR );

//    cascade.writeDOT( "mrcascade.dot" );

    cascade.complete();

    validateLength( thirdMR.openSink(), 10 );
    }

  }
