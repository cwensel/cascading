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

package cascading.flow.hadoop;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import cascading.PlatformTestCase;
import cascading.flow.hadoop.planner.HadoopPlanner;
import cascading.platform.hadoop.BaseHadoopPlatform;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.util.Util;
import data.InputData;
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
public class MultiMapReduceFlowPlatformTest extends PlatformTestCase
  {
  public MultiMapReduceFlowPlatformTest()
    {
    super( true );
    }

  @Test
  public void testFlow() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    String outputPath1 = getOutputPath( "flowTest1" );
    String outputPath2 = getOutputPath( "flowTest2" );
    String outputPath3 = getOutputPath( "flowTest3" );

    remove( outputPath1, true );
    remove( outputPath2, true );
    remove( outputPath3, true );

    JobConf defaultConf = (JobConf) ( (BaseHadoopPlatform) getPlatform() ).getConfiguration();

    JobConf conf1 = createJob( defaultConf, "mr1", InputData.inputFileApache, outputPath1 );
    JobConf conf2 = createJob( defaultConf, "mr2", outputPath1, outputPath2 );
    JobConf conf3 = createJob( defaultConf, "mr3", outputPath2, outputPath3 );

    MultiMapReduceFlow flow = new MultiMapReduceFlow( "mrflow", conf1, conf2, conf3 );

    validateLength( new Hfs( new TextLine(), InputData.inputFileApache ).openForRead( new HadoopFlowProcess( defaultConf ) ), 10 );

    flow.complete();

    validateLength( new Hfs( new TextLine(), outputPath1 ).openForRead( new HadoopFlowProcess( defaultConf ) ), 10 );

    Collection<Tap> sinks = flow.getSinks().values();

    assertEquals( 1, sinks.size() );
    String identifier = sinks.iterator().next().getIdentifier();
    assertEquals( "flowTest3", identifier.substring( identifier.lastIndexOf( '/' ) + 1 ) );
    }

  @Test
  public void testFlowLazy() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    String outputPath1 = getOutputPath( "flowTest1" );
    String outputPath2 = getOutputPath( "flowTest2" );
    String outputPath3 = getOutputPath( "flowTest3" );

    remove( outputPath1, true );
    remove( outputPath2, true );
    remove( outputPath3, true );

    JobConf defaultConf = (JobConf) ( (BaseHadoopPlatform) getPlatform() ).getConfiguration();

    JobConf conf1 = createJob( defaultConf, "mr1", InputData.inputFileApache, outputPath1 );
    JobConf conf2 = createJob( defaultConf, "mr2", outputPath1, outputPath2 );
    JobConf conf3 = createJob( defaultConf, "mr3", outputPath2, outputPath3 );

    validateLength( new Hfs( new TextLine(), InputData.inputFileApache ).openForRead( new HadoopFlowProcess( defaultConf ) ), 10 );

    MultiMapReduceFlow flow = new MultiMapReduceFlow( "mrflow", conf1 );

    flow.start();

    Util.safeSleep( 3000 );
    flow.attachFlowStep( conf2 );

    Util.safeSleep( 3000 );
    flow.attachFlowStep( conf3 );

    flow.complete();

    validateLength( new Hfs( new TextLine(), outputPath1 ).openForRead( new HadoopFlowProcess( defaultConf ) ), 10 );

    Collection<Tap> sinks = flow.getSinks().values();

    assertEquals( 1, sinks.size() );
    String identifier = sinks.iterator().next().getIdentifier();
    assertEquals( "flowTest3", identifier.substring( identifier.lastIndexOf( '/' ) + 1 ) );
    }

  @Test(expected = IllegalStateException.class)
  public void testFlowLazyFail() throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    String outputPath1 = getOutputPath( "flowTest1" );
    String outputPath2 = getOutputPath( "flowTest2" );

    remove( outputPath1, true );
    remove( outputPath2, true );

    JobConf defaultConf = (JobConf) ( (BaseHadoopPlatform) getPlatform() ).getConfiguration();

    JobConf conf1 = createJob( defaultConf, "mr1", InputData.inputFileApache, outputPath1 );
    JobConf conf2 = createJob( defaultConf, "mr2", outputPath1, outputPath2 );

    validateLength( new Hfs( new TextLine(), InputData.inputFileApache ).openForRead( new HadoopFlowProcess( defaultConf ) ), 10 );

    MultiMapReduceFlow flow = new MultiMapReduceFlow( "mrflow", conf1 );

    flow.complete();

    flow.attachFlowStep( conf2 );
    }

  protected JobConf createJob( JobConf defaultConf, String name, String inputPath, String outputPath )
    {
    JobConf conf = new JobConf( defaultConf );
    conf.setJobName( name );

    conf.setOutputKeyClass( LongWritable.class );
    conf.setOutputValueClass( Text.class );

    conf.setMapperClass( IdentityMapper.class );
    conf.setReducerClass( IdentityReducer.class );

    conf.setInputFormat( TextInputFormat.class );
    conf.setOutputFormat( TextOutputFormat.class );

    FileInputFormat.setInputPaths( conf, new Path( inputPath ) );
    FileOutputFormat.setOutputPath( conf, new Path( outputPath ) );
    return conf;
    }

  private String remove( String path, boolean delete ) throws IOException
    {
    FileSystem fs = FileSystem.get( URI.create( path ), HadoopPlanner.createJobConf( getProperties() ) );

    if( delete )
      fs.delete( new Path( path ), true );

    return path;
    }
  }
