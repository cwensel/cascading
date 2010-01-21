/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import cascading.ClusterTestCase;
import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
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
  String outputPath = "build/test/output/mapreduceflow/";
  String outputPath1 = "build/test/output/mapreducecascade1/";
  String outputPath2 = "build/test/output/mapreducecascade2/";
  String outputPath3 = "build/test/output/mapreducecascade3/";

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
    FileOutputFormat.setOutputPath( conf, new Path( outputPath ) );

    Flow flow = new MapReduceFlow( "mrflow", conf, true );

    validateLength( flow.openSource(), 10 );

    flow.complete();

    validateLength( flow.openSink(), 10 );
    }

  public void testCascade() throws IOException
    {
    if( !new File( inputFileApache ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileApache );

    JobConf defaultConf = MultiMapReducePlanner.getJobConf( getProperties() );

    JobConf firstConf = new JobConf( defaultConf );
    firstConf.setJobName( "first" );

    firstConf.setOutputKeyClass( LongWritable.class );
    firstConf.setOutputValueClass( Text.class );

    firstConf.setMapperClass( IdentityMapper.class );
    firstConf.setReducerClass( IdentityReducer.class );

    firstConf.setInputFormat( TextInputFormat.class );
    firstConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( firstConf, new Path( inputFileApache ) );
    FileOutputFormat.setOutputPath( firstConf, new Path( outputPath1 ) );

    Flow firstFlow = new MapReduceFlow( firstConf, true );

    JobConf secondConf = new JobConf( defaultConf );
    secondConf.setJobName( "second" );

    secondConf.setOutputKeyClass( LongWritable.class );
    secondConf.setOutputValueClass( Text.class );

    secondConf.setMapperClass( IdentityMapper.class );
    secondConf.setReducerClass( IdentityReducer.class );

    secondConf.setInputFormat( TextInputFormat.class );
    secondConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( secondConf, new Path( outputPath1 ) );
    FileOutputFormat.setOutputPath( secondConf, new Path( outputPath2 ) );

    Flow secondFlow = new MapReduceFlow( secondConf, true );

    JobConf thirdConf = new JobConf( defaultConf );
    thirdConf.setJobName( "third" );

    thirdConf.setOutputKeyClass( LongWritable.class );
    thirdConf.setOutputValueClass( Text.class );

    thirdConf.setMapperClass( IdentityMapper.class );
    thirdConf.setReducerClass( IdentityReducer.class );

    thirdConf.setInputFormat( TextInputFormat.class );
    thirdConf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( thirdConf, new Path( outputPath2 ) );
    FileOutputFormat.setOutputPath( thirdConf, new Path( outputPath3 ) );

    Flow thirdFlow = new MapReduceFlow( thirdConf, true );

    CascadeConnector cascadeConnector = new CascadeConnector();

    // pass out of order
    Cascade cascade = cascadeConnector.connect( thirdFlow, firstFlow, secondFlow );

    cascade.complete();

    validateLength( thirdFlow.openSink(), 10 );
    }
  }
