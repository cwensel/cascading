/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

public class MergePipesPlatformTest extends PlatformTestCase
  {
  public MergePipesPlatformTest()
    {
    super( true ); // leave cluster testing enabled
    }

  @Test
  public void testSimpleMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simplemerge" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    }

  /**
   * Confirms support for Merge->GroupBy
   * <p>
   * On Tez, this results in an identity node
   * <p>
   * TODO: tez planner logical optimization - remove Merge and push 'merge' to GroupBy
   */
  @Test
  public void testMergeGroupBy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap sourceUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath(), SinkMode.REPLACE );

    Pipe pipeLower = new Pipe( "lower" );
    Pipe pipeUpper = new Pipe( "upper" );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new GroupBy( splice, Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    }

  /**
   * Specifically tests GroupBy will return the correct grouping fields to the following Every
   *
   * @throws Exception
   */
  @Test
  public void testSimpleMergeThree() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethree" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper, pipeOffset );

    splice = new Each( splice, new Fields( "num", "char" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChain() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechain" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new Merge( splice, pipeOffset );

    splice = new Each( splice, new Fields( "num", "char" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChainGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechaingroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new Merge( splice, pipeOffset );

    splice = new GroupBy( splice, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChainCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechaincogroup" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new CoGroup( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 6 );
    }

  @Test
  public void testSameSourceMergeThreeChainGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );

    Map sources = new HashMap();

    sources.put( "split", sourceLower );

    Tap sink = getPlatform().getTextFile( getOutputPath( "samemergethreechaingroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipe = new Pipe( "split" );

    Pipe pipeLower = new Each( new Pipe( "lower", pipe ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper", pipe ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset", pipe ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    //put group before merge to test path counts
    splice = new GroupBy( splice, new Fields( "num" ) );

    splice = new Merge( splice, pipeOffset );

    // this group has its incoming paths counted, gated by the previous group
    splice = new GroupBy( splice, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 15 );
    }

  @Test
  public void testSplitSameSourceMerged() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemerged" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new Merge( "merged", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testSplitSameSourceMergedComplex() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemergedcomplex" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new Merge( "merged-first", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    left = new Each( new Pipe( "left", merged ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    right = new Each( new Pipe( "right", merged ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    merged = new Merge( "merged-second", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 3 );
    }

  /**
   * Verifies field names are consistent across streams
   */
  @Test
  public void testSimpleMergeFail() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simplemergefail" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Rename( pipeLower, new Fields( "num" ), new Fields( "num2" ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // ignore
      }
    }

  @Test
  public void testMergeIntoHashJoinStreamed() throws Exception
    {
    runMergeIntoHashJoin( true );
    }

  @Test
  public void testMergeIntoHashJoinAccumulated() throws Exception
    {
    runMergeIntoHashJoin( false );
    }

  private void runMergeIntoHashJoin( boolean streamed ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = streamed ? "streamed" : "accumulated";
    String path = "mergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    if( streamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    // two jobs, we must put a temp tap between the Merge and HashJoin
    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 6 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamed() throws Exception
    {
    runHashJoinIntoMergeIntoHashJoin( true );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulated() throws Exception
    {
    runHashJoinIntoMergeIntoHashJoin( false );
    }

  private void runHashJoinIntoMergeIntoHashJoin( boolean streamed ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = streamed ? "streamed" : "accumulated";
    String path = "hashjoinintomergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge", splice, pipeUpper );

    if( streamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", streamed ? 1 : 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 8 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedStreamedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, true, true, 1 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedAccumulatedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, false, true, 3 );
    }

  /**
   * This test will exercise the issue where a unconnected HashJoin could be accumulated against within
   * a node.
   */
  @Test
  public void testHashJoinMergeIntoHashJoinStreamedAccumulatedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, false, true, 2 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedStreamedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, true, true, 3 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedStreamed() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, true, false, 1 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedAccumulated() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, false, false, 3 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedAccumulated() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, false, false, 2 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedStreamed() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, true, false, 3 );
    }

  private void runMultiHashJoinIntoMergeIntoHashJoin( boolean firstStreamed, boolean secondStreamed, boolean interMerge, int expectedSteps ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = firstStreamed ? "firstStreamed" : "firstAccumulated";
    name += secondStreamed ? "secondStreamed" : "secondAccumulated";
    name += interMerge ? "interMerge" : "noInterMerge";

    String path = "multihashjoinintomergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge1", splice, pipeUpper );

    if( firstStreamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    if( interMerge )
      splice = new Merge( "merge2", splice, pipeUpper );

    if( secondStreamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", expectedSteps, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, interMerge ? 17 : 14 );
    }

  @Test
  public void testGroupByAggregationMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath(), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );
    pipeLower = new Every( pipeLower, new Fields( "char" ), new First( Fields.ARGS ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );
    pipeUpper = new Every( pipeUpper, new Fields( "char" ), new First( Fields.ARGS ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    }

  @Test
  public void testSameSourceMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhs = new Pipe( "lhs" );
    lhs = new Pipe( "lhs", lhs ); // rule should catch adjacent pipes

    Pipe rhs = new Pipe( "rhs" );

    Pipe merge = new Merge( "merge", lhs, rhs );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( lhs, source )
      .addSource( rhs, source )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testSameSourceMergeHashJoin() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe mergeLhs = new Pipe( "lhs" );
    Pipe mergeRhs = new Pipe( "rhs" );

    Pipe mergePipe = new Merge( "merge", mergeLhs, mergeRhs );

    mergePipe = new Rename( mergePipe, new Fields( "num", "char" ), new Fields( "merged.num", "merged.char" ) );

    Pipe joinRhs = new Pipe( "join" );
    joinRhs = new Rename( joinRhs, new Fields( "num", "char" ), new Fields( "rhs.num", "rhs.char" ) );

    Pipe lookupJoin = new HashJoin( mergePipe, new Fields( "merged.num" ), joinRhs, new Fields( "rhs.num" ) );

    Pipe retain = new Retain( lookupJoin, new Fields( "merged.num", "merged.char", "rhs.char" ) );

    Pipe out = new Rename( retain, new Fields( "merged.num", "merged.char", "rhs.char" ), new Fields( "num", "merged", "char" ) );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( mergeLhs, source )
      .addSource( mergeRhs, source )
      .addSource( joinRhs, source )
      .addTailSink( out, sink );

    FlowConnector flowConnector = getPlatform().getFlowConnector();

    Flow flow = flowConnector.connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testHashJoinMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap lower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap upper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );
    Tap offset = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhs = new Pipe( "lhs" );
    Pipe mid = new Pipe( "mid" );
    Pipe rhs = new Pipe( "rhs" );

    mid = new Rename( mid, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe join = new HashJoin( lhs, new Fields( "num" ), mid, new Fields( "num2" ) );

    join = new Retain( join, new Fields( "num", "char" ) );

    Pipe merge = new Merge( "merge", join, rhs );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( lhs, lower )
      .addSource( mid, upper )
      .addSource( rhs, offset )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 9 );
    }

  /**
   * TODO: tez plan optimization - remove Boundary insertion and rely on DistCacheTap rule to provide broadcast
   * <p>
   * BottomUpJoinedBoundariesNodePartitioner
   */
  @Test
  public void testSameSourceHashJoinMergeOnStreamed() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap lower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap upper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhs = new Pipe( "lhs" );
    Pipe accumulated = new Pipe( "accumulated" );
    Pipe rhs = new Pipe( "rhs" );

    Pipe join = new HashJoin( lhs, new Fields( "num" ), accumulated, new Fields( "num" ), new Fields( "num", "char", "num2", "char2" ) );

    join = new Retain( join, new Fields( "num", "char" ) );

    Pipe merge = new Merge( "merge", join, rhs );

    // split feeds hashjoin streamed side
    FlowDef flowDef = FlowDef.flowDef()
      .setName( "streamed" )
      .addSource( lhs, lower )
      .addSource( accumulated, upper )
      .addSource( rhs, lower )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }

  /**
   * TODO: tez plan optimization - remove Boundary insertion and rely on DistCacheTap rule to provide broadcast
   * <p>
   * BottomUpJoinedBoundariesNodePartitioner
   */
  @Test
  public void testSameSourceHashJoinMergeOnAccumulated() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap lower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap upper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhs = new Pipe( "lhs" );
    Pipe accumulated = new Pipe( "accumulated" );
    Pipe rhs = new Pipe( "rhs" );

    Pipe join = new HashJoin( lhs, new Fields( "num" ), accumulated, new Fields( "num" ), new Fields( "num", "char", "num2", "char2" ) );

    join = new Retain( join, new Fields( "num", "char" ) );

    Pipe merge = new Merge( "merge", join, rhs );

    // split feeds hashjoin accumulated side
    FlowDef flowDef = FlowDef.flowDef()
      .setName( "accumulated" )
      .addSource( lhs, upper )
      .addSource( accumulated, lower )
      .addSource( rhs, lower )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }

  /**
   * Currently not supported by Tez
   */
  @Test
  public void testHashJoinHashJoinMerge() throws Exception
    {
    if( getPlatform().isDAG() )
      return;

    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap lhsLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap lhsUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );
    Tap rhsLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap rhsUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhsLowerPipe = new Pipe( "lhsLower" );
    Pipe lhsUpperPipe = new Pipe( "lhsUpper" );
    Pipe rhsLowerPipe = new Pipe( "rhsLower" );
    Pipe rhsUpperPipe = new Pipe( "rhsUpper" );

    lhsUpperPipe = new Rename( lhsUpperPipe, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe lhs = new HashJoin( lhsLowerPipe, new Fields( "num" ), lhsUpperPipe, new Fields( "num2" ) );

    lhs = new Retain( lhs, new Fields( "num", "char" ) );

    rhsUpperPipe = new Rename( rhsUpperPipe, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe rhs = new HashJoin( rhsLowerPipe, new Fields( "num" ), rhsUpperPipe, new Fields( "num2" ) );

    rhs = new Retain( rhs, new Fields( "num", "char" ) );

    Pipe merge = new Merge( "merge", lhs, rhs );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( lhsLowerPipe, lhsLower )
      .addSource( lhsUpperPipe, lhsUpper )
      .addSource( rhsLowerPipe, rhsLower )
      .addSource( rhsUpperPipe, rhsUpper )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 10 );
    }

  /**
   * Tests for https://github.com/cwensel/cascading/issues/61
   * Currently not supported by Tez
   */
  @Test
  public void testHashJoinHashJoinHashJoinMergeMerge() throws Exception
    {
    if( getPlatform().isDAG() )
      return;

    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap lhsLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap lhsUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );
    Tap midLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap midUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );
    Tap rhsLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap rhsUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );
    Tap far = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath(), SinkMode.REPLACE );

    Pipe lhsLowerPipe = new Pipe( "lhsLower" );
    Pipe lhsUpperPipe = new Pipe( "lhsUpper" );
    Pipe midLowerPipe = new Pipe( "midLower" );
    Pipe midUpperPipe = new Pipe( "midUpper" );
    Pipe rhsLowerPipe = new Pipe( "rhsLower" );
    Pipe rhsUpperPipe = new Pipe( "rhsUpper" );
    Pipe farPipe = new Pipe( "far" );

    lhsUpperPipe = new Rename( lhsUpperPipe, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe lhs = new HashJoin( lhsLowerPipe, new Fields( "num" ), lhsUpperPipe, new Fields( "num2" ) );

    lhs = new Retain( lhs, new Fields( "num", "char" ) );

    midUpperPipe = new Rename( midUpperPipe, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe mid = new HashJoin( midLowerPipe, new Fields( "num" ), midUpperPipe, new Fields( "num2" ) );

    mid = new Retain( mid, new Fields( "num", "char" ) );

    rhsUpperPipe = new Rename( rhsUpperPipe, Fields.ALL, new Fields( "num2", "char2" ) );

    Pipe rhs = new HashJoin( rhsLowerPipe, new Fields( "num" ), rhsUpperPipe, new Fields( "num2" ) );

    rhs = new Retain( rhs, new Fields( "num", "char" ) );

    Pipe merge = new Merge( "merge", lhs, mid, rhs );

    merge = new Merge( "next merge", merge, farPipe );

    FlowDef flowDef = FlowDef.flowDef()
      .addSource( lhsLowerPipe, lhsLower )
      .addSource( lhsUpperPipe, lhsUpper )
      .addSource( midLowerPipe, midLower )
      .addSource( midUpperPipe, midUpper )
      .addSource( rhsLowerPipe, rhsLower )
      .addSource( rhsUpperPipe, rhsUpper )
      .addSource( farPipe, far )
      .addTailSink( merge, sink );

    Flow flow = getPlatform().getFlowConnector().connect( flowDef );

    flow.complete();

    validateLength( flow, 19 );
    }
  }
