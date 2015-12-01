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

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import data.InputData;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobInProgress;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Test;

import static data.InputData.inputFileLower;
import static data.InputData.inputFileUpper;

/**
 *
 */
public class HadoopMR1TapPlatformTest extends PlatformTestCase implements Serializable
  {
  public HadoopMR1TapPlatformTest()
    {
    super( true );
    }

  @Test
  public void testCombinedHfs() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Hfs sourceLower = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileLower );
    Hfs sourceUpper = new Hfs( new TextLine( new Fields( "offset", "line" ) ), InputData.inputFileUpper );

    // create a CombinedHfs instance on these files
    Tap source = new MultiSourceTap<Hfs, JobConf, RecordReader>( sourceLower, sourceUpper );

    FlowProcess<JobConf> process = getPlatform().getFlowProcess();
    JobConf conf = process.getConfigCopy();

    // set the combine flag
    conf.setBoolean( HfsProps.COMBINE_INPUT_FILES, true );

    conf.set( "cascading.flow.platform", "hadoop" ); // only supported on mr based platforms

    // test the input format and the split
    source.sourceConfInit( process, conf );

    InputFormat inputFormat = conf.getInputFormat();

    assertEquals( Hfs.CombinedInputFormat.class, inputFormat.getClass() );
    InputSplit[] splits = inputFormat.getSplits( conf, 1 );

    assertEquals( 1, splits.length );

    validateLength( source.openForRead( process ), 10 );
    }

  @Test
  public void testCombinedPartitionTap() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), " ", inputFileLower );

    Tap partitionTap = getPlatform().getDelimitedFile( new Fields( "lower" ), "+", getOutputPath( "/combinedpartition/partitioned" ), SinkMode.REPLACE );

    Partition partition = new DelimitedPartition( new Fields( "number" ) );
    partitionTap = getPlatform().getPartitionTap( partitionTap, partition, 1 );

    Flow firstFlow = getPlatform().getFlowConnector().connect( source, partitionTap, new Pipe( "partition" ) );

    firstFlow.complete();

    // Configure combine inputs for reading from the partition tap
    Map<Object, Object> properties = getProperties();
    HfsProps.setUseCombinedInput( properties, true );
    //set to lots of bytes so the test will combine all input files
    HfsProps.setCombinedInputMaxSize( properties, 100000000L );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), "+", getOutputPath( "/combinedpartition/final" ), SinkMode.REPLACE );

    Flow secondFlow = getPlatform().getFlowConnector( properties ).connect( partitionTap, sink, new Pipe( "copy" ) );

    secondFlow.complete();

    //Asserting we combined all partition files into one mapper
    if( getPlatform().isUseCluster() )
      assertEquals( 1, secondFlow.getStats().getCounterValue( JobInProgress.Counter.TOTAL_LAUNCHED_MAPS ) );

    List<Tuple> values = getSinkAsList( secondFlow );
    assertEquals( 5, values.size() );
    assertTrue( values.contains( new Tuple( "1", "a" ) ) );
    assertTrue( values.contains( new Tuple( "2", "b" ) ) );
    assertTrue( values.contains( new Tuple( "3", "c" ) ) );
    assertTrue( values.contains( new Tuple( "4", "d" ) ) );
    assertTrue( values.contains( new Tuple( "5", "e" ) ) );
    }

  @Test
  public void testFilteredPartitionTap_Typical() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), " ", inputFileLower );

    Tap delimitedFile = getPlatform().getDelimitedFile( new Fields( "lower" ), "+", getOutputPath( "/filteredpartition/partitioned" ), SinkMode.REPLACE );

    Partition partition = new DelimitedPartition( new Fields( "number" ) );
    PartitionTap partitionTap = (PartitionTap) getPlatform().getPartitionTap( delimitedFile, partition, 1 );

    Flow firstFlow = getPlatform().getFlowConnector().connect( source, partitionTap, new Pipe( "partition" ) );

    firstFlow.complete();

    partitionTap = (PartitionTap) getPlatform().getPartitionTap( delimitedFile, partition, 1 );

    partitionTap.addSourcePartitionFilter( new Fields( "number" ), new PartitionFilter( Arrays.asList( "2", "4" ) ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), "+", getOutputPath( "/filteredpartition/final" ), SinkMode.REPLACE );

    Flow secondFlow = getPlatform().getFlowConnector().connect( partitionTap, sink, new Pipe( "copy" ) );

    secondFlow.complete();

    List<Tuple> values = getSinkAsList( secondFlow );
    assertEquals( 3, values.size() );
    assertTrue( values.contains( new Tuple( "1", "a" ) ) );
    assertTrue( values.contains( new Tuple( "3", "c" ) ) );
    assertTrue( values.contains( new Tuple( "5", "e" ) ) );
    }

  @Test
  public void testFilteredPartitionTap_NoFilters() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), " ", inputFileLower );

    Tap delimitedFile = getPlatform().getDelimitedFile( new Fields( "lower" ), "+", getOutputPath( "/filteredpartition/partitioned" ), SinkMode.REPLACE );

    Partition partition = new DelimitedPartition( new Fields( "number" ) );
    Tap partitionTap = getPlatform().getPartitionTap( delimitedFile, partition, 1 );

    Flow firstFlow = getPlatform().getFlowConnector().connect( source, partitionTap, new Pipe( "partition" ) );

    firstFlow.complete();

    partitionTap = getPlatform().getPartitionTap( delimitedFile, partition, 1 );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "number", "lower" ), "+", getOutputPath( "/filteredpartition/final" ), SinkMode.REPLACE );

    Flow secondFlow = getPlatform().getFlowConnector().connect( partitionTap, sink, new Pipe( "copy" ) );

    secondFlow.complete();

    List<Tuple> values = getSinkAsList( secondFlow );
    assertEquals( 5, values.size() );
    assertTrue( values.contains( new Tuple( "1", "a" ) ) );
    assertTrue( values.contains( new Tuple( "2", "b" ) ) );
    assertTrue( values.contains( new Tuple( "3", "c" ) ) );
    assertTrue( values.contains( new Tuple( "4", "d" ) ) );
    assertTrue( values.contains( new Tuple( "5", "e" ) ) );
    }

  @Test
  public void testFilteredPartitionTap_SameNameWithType() throws Exception
    {
    Fields partitionFields = new Fields( "name", String.class );
    Fields argumentSelector = new Fields( "name" );
    testFilteredPartitionTapFields( partitionFields, argumentSelector );
    }

  @Test
  public void testFilteredPartitionTap_SameNameNoTypes() throws Exception
    {
    Fields partitionFields = new Fields( "name" );
    Fields argumentSelector = new Fields( "name" );
    testFilteredPartitionTapFields( partitionFields, argumentSelector );
    }

  @Test(expected = FieldsResolverException.class)
  public void testFilteredPartitionTap_DifferentNameNoType() throws Exception
    {
    Fields partitionFields = new Fields( "name1" );
    Fields argumentSelector = new Fields( "name2" );
    testFilteredPartitionTapFields( partitionFields, argumentSelector );
    }

  @Test(expected = FieldsResolverException.class)
  public void testFilteredPartitionTap_DifferentNameWithTYpe() throws Exception
    {
    Fields partitionFields = new Fields( "name1", String.class );
    Fields argumentSelector = new Fields( "name2" );
    testFilteredPartitionTapFields( partitionFields, argumentSelector );
    }

  private void testFilteredPartitionTapFields( Fields partitionFields, Fields argumentSelector ) throws Exception
    {
    Tap tap = getPlatform().getTextFile( "dummy" );
    Partition partition = new DelimitedPartition( partitionFields );
    PartitionTap partitionTap = (PartitionTap) getPlatform().getPartitionTap( tap, partition, 1 );
    partitionTap.addSourcePartitionFilter( argumentSelector, new TrueFilter() );
    }

  static class TrueFilter extends BaseOperation implements Filter
    {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      return true;
      }

    }

  static class PartitionFilter extends BaseOperation implements Filter
    {
    private static final long serialVersionUID = 1L;

    private final List<String> partitions;

    public PartitionFilter( List<String> partitions )
      {
      this.partitions = partitions;
      }

    @Override
    public boolean isRemove( FlowProcess flowProcess, FilterCall filterCall )
      {
      return partitions.contains( filterCall.getArguments().getString( "number" ) );
      }
    }
  }
