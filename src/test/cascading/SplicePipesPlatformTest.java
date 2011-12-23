/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class SplicePipesPlatformTest extends PlatformTestCase
  {
  public SplicePipesPlatformTest()
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

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSplitSameSourceMerged() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemerged" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new Merge( "merged", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    flow.complete();

    validateLength( flow, 3 );
    }

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
  }
