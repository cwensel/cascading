/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

package cascading.cascade;

import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Dfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/CascadeTest.java#2 $ */
public class CascadeTest extends ClusterTestCase
  {
  String inputFile = "build/test/data/ips.20.txt";
  String outputPath = "build/test/output/cascade/";

  public CascadeTest()
    {
    super( "cascade tests", true );
    }

  private Flow firstFlow()
    {
    Tap source = new Dfs( new TextLine( new Fields( "offset", "line" ) ), inputFile );

    Pipe pipe = new Pipe( "first" );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = new Dfs( new SequenceFile( new Fields( "ip" ) ), outputPath + "/first", true );

    return new FlowConnector( jobConf ).connect( source, sink, pipe );
    }

  private Flow secondFlow( Tap source )
    {
    Pipe pipe = new Pipe( "second" );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );

    Tap sink = new Dfs( new SequenceFile( new Fields( "first", "second", "third", "fourth" ) ), outputPath + "/second", true );

    return new FlowConnector( jobConf ).connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap source )
    {
    Pipe pipe = new Pipe( "third" );

    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = new Dfs( new SequenceFile( new Fields( "mangled" ) ), outputPath + "/third", true );

    return new FlowConnector( jobConf ).connect( source, sink, pipe );
    }

  private Flow fourthFlow( Tap source )
    {
    Pipe pipe = new Pipe( "fourth" );

    pipe = new Each( pipe, new Identity() );

    Tap sink = new Dfs( new TextLine(), outputPath + "/fourth", true );

    return new FlowConnector( jobConf ).connect( source, sink, pipe );
    }

  public void testSimpleCascade() throws IOException
    {
    copyFromLocal( inputFile );

    Flow first = firstFlow();
    Flow second = secondFlow( first.getSink() );
    Flow third = thirdFlow( second.getSink() );
    Flow fourth = fourthFlow( third.getSink() );

    Cascade cascade = new CascadeConnector().connect( first, second, third, fourth );

    cascade.start();

    cascade.complete();


    validateLength( fourth, 20 );
    }
  }
