/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.operation.Function;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

/**
 * Tests for DistCacheTap.
 */
public class DistCacheTapPlatformTest extends PlatformTestCase implements Serializable
  {
  public DistCacheTapPlatformTest()
    {
    super( true );
    }

  @Test
  public void testHashJoinDistCacheTapRHS() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = new DistCacheTap( (Hfs) getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper ) );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( getTestName() + "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( "distcache test", sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  @Test
  public void testHashJoinDistCacheTapLHS() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = new DistCacheTap( (Hfs) getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower ) );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( getTestName() + "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( "distcache test", sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  @Test
  public void testHashJoinCheckpointWithDistCacheDecorator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new Checkpoint( pipeUpper );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();
    FlowConnectorProps.setCheckpointTapDecoratorClass( properties, DistCacheTap.class.getName() );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testGlobSupport() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    File dir = File.createTempFile( "distcachetap", Long.toString( System.nanoTime() ) );
    if( dir.exists() )
      {
      if( dir.isDirectory() )
        FileUtils.deleteDirectory( dir );
      else
        dir.delete();
      }
    dir.mkdirs();
    String[] data = new String[]{"1 A", "2 B", "3 C", "4 D", "5 E"};
    for( int i = 0; i < 5; i++ )
      {
      FileWriter fw = new FileWriter( new File( dir.getAbsolutePath(), "upper_" + i + ".txt" ) );
      fw.write( data[ i ] );
      fw.close();
      }
    dir.deleteOnExit();

    getPlatform().copyFromLocal( dir.getAbsolutePath() );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = new DistCacheTap( (Hfs)
      getPlatform().getTextFile( new Fields( "offset", "line" ), dir.getAbsolutePath() + "/*" ) );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( getTestName() + "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( "distcache test", sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  @Test
  public void testDirectory() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    File dir = File.createTempFile( "distcachetap", Long.toString( System.nanoTime() ) );
    if( dir.exists() )
      {
      if( dir.isDirectory() )
        FileUtils.deleteDirectory( dir );
      else
        dir.delete();
      }
    dir.mkdirs();
    String[] data = new String[]{"1 A", "2 B", "3 C", "4 D", "5 E"};
    FileWriter fw = new FileWriter( new File( dir.getAbsolutePath(), "upper.txt" ) );
    for( int i = 0; i < 5; i++ )
      fw.write( data[ i ] + System.getProperty( "line.separator" ) );

    fw.close();

    getPlatform().copyFromLocal( dir.getAbsolutePath() );

    dir.deleteOnExit();

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = new DistCacheTap( (Hfs)
      getPlatform().getTextFile( new Fields( "offset", "line" ), dir.getAbsolutePath() ) );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( getTestName() + "join" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    Map<Object, Object> properties = getProperties();

    Flow flow = getPlatform().getFlowConnector( properties ).connect( "distcache test", sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    assertTrue( values.contains( new Tuple( "3\tc\t3\tC" ) ) );
    assertTrue( values.contains( new Tuple( "4\td\t4\tD" ) ) );
    assertTrue( values.contains( new Tuple( "5\te\t5\tE" ) ) );
    }

  }
