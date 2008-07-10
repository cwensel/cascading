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
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextLine;
import cascading.tap.Dfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/** @version $Id: //depot/calku/cascading/src/test/cascading/CascadeTest.java#2 $ */
public class ParallelCascadeTest extends ClusterTestCase
  {
  String inputFile = "build/test/data/ips.20.txt";
  String outputPath = "build/test/output/parallelcascade/";

  public ParallelCascadeTest()
    {
    super( "parallel cascade tests", true );
    }

  private Flow firstFlow( String name )
    {
    Tap source = new Dfs( new TextLine( new Fields( "offset", "line" ) ), inputFile );

    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = new Dfs( new SequenceFile( new Fields( "ip" ) ), outputPath + "/" + name, true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow secondFlow( String name, Tap source )
    {
    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );
    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = new Dfs( new SequenceFile( new Fields( "mangled" ) ), outputPath + "/" + name, true );

    return new FlowConnector( getProperties() ).connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap lhs, Tap rhs )
    {
    Pipe lhsPipe = new Pipe( "lhs" );
    Pipe rhsPipe = new Pipe( "rhs" );

    Pipe pipe = new CoGroup( lhsPipe, new Fields( 0 ), rhsPipe, new Fields( 0 ), Fields.size( 2 ) );

    Tap sink = new Dfs( new TextLine(), outputPath + "/fourth", true );

    return new FlowConnector( getProperties() ).connect( Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) ), sink, pipe );
    }

  public void testCascade() throws IOException
    {
    copyFromLocal( inputFile );

    Flow first1 = firstFlow( "first1" );
    Flow second1 = secondFlow( "second1", first1.getSink() );

    Flow first2 = firstFlow( "first2" );
    Flow second2 = secondFlow( "second2", first2.getSink() );

    Flow third = thirdFlow( second1.getSink(), second2.getSink() );

    Cascade cascade = new CascadeConnector().connect( first1, second1, first2, second2, third );

    cascade.start();

    cascade.complete();

    validateLength( third, 28 );
    }
  }