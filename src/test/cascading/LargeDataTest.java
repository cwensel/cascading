/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import java.io.File;
import java.util.Map;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
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
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/LargeDataTest.java#3 $ */
public class LargeDataTest extends ClusterTestCase
  {
  String inputPageData = "build/test/data/url+page.200.txt";

  String outputPathUrl = "build/test/output/large/url";
  String outputPathWord = "build/test/output/large/word";

  public LargeDataTest()
    {
    super( "large data", true );
    }

  public void testLargeDataSet() throws Exception
    {
    if( !new File( inputPageData ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputPageData );

    Tap source = new Hfs( new TextLine(), inputPageData );
    Tap sinkUrl = new Hfs( new TextLine(), outputPathUrl, true );
    Tap sinkWord = new Hfs( new TextLine(), outputPathWord, true );

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

    Flow flow = new FlowConnector( getProperties() ).connect( sources, sinks, Pipe.pipes( pipeUrl, pipeWord ) );

    //    flow.writeDOT( "large.dot" );

    flow.complete();

    validateLength( flow, 23807 );
    }
  }