/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.regex.RegexReplace;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.xml.TagSoupParser;
import cascading.operation.xml.XPathGenerator;
import cascading.operation.xml.XPathOperation;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

import static data.InputData.inputPageData;

@PlatformTest(platforms = {"local", "hadoop"})
public class LargeDataTest extends PlatformTestCase
  {
  public LargeDataTest()
    {
    super( true );
    }

  public void testLargeDataSet() throws Exception
    {
    getPlatform().copyFromLocal( inputPageData );

    Tap source = getPlatform().getTextFile( inputPageData );
    Tap sinkUrl = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "url" ), SinkMode.REPLACE );
    Tap sinkWord = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "word" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "large" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "url", "raw" ) ) );
    pipe = new Each( pipe, new Fields( "url" ), new RegexFilter( ".*\\.pdf$", true ) );
    pipe = new Each( pipe, new Fields( "raw" ), new RegexReplace( new Fields( "page" ), ":nl:", "\n" ), new Fields( "url", "page" ) );
    pipe = new Each( pipe, new Fields( "page" ), new TagSoupParser( new Fields( "xml" ) ), new Fields( "url", "xml" ) );
    pipe = new Each( pipe, new Fields( "xml" ), new XPathGenerator( new Fields( "body" ), XPathOperation.NAMESPACE_XHTML, "//xhtml:body" ), new Fields( "url", "body" ) );
    pipe = new Each( pipe, new Fields( "body" ), new XPathGenerator( new Fields( "words" ), XPathOperation.NAMESPACE_XHTML, "//text()[ name(parent::node()) != 'script']" ), new Fields( "url", "words" ) );
    pipe = new Each( pipe, new Fields( "words" ), new RegexGenerator( new Fields( "word" ), "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)" ), new Fields( "url", "word" ) );

    Pipe pipeUrl = new GroupBy( "url", pipe, new Fields( "url", "word" ) );
    pipeUrl = new Every( pipeUrl, new Fields( "url", "word" ), new Count(), new Fields( "url", "word", "count" ) );

    Pipe pipeWord = new GroupBy( "word", pipe, new Fields( "word" ) );
    pipeWord = new Every( pipeWord, new Fields( "word" ), new Count(), new Fields( "word", "count" ) );

    Map<String, Tap> sources = Cascades.tapsMap( Pipe.pipes( pipe ), Tap.taps( source ) );
    Map<String, Tap> sinks = Cascades.tapsMap( Pipe.pipes( pipeUrl, pipeWord ), Tap.taps( sinkUrl, sinkWord ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, Pipe.pipes( pipeUrl, pipeWord ) );

    flow.complete();

    validateLength( flow, 23807, "word" );
    }
  }