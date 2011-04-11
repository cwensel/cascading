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

package cascading.cascade;

import java.io.IOException;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.operation.Identity;
import cascading.operation.regex.RegexSplitter;
import cascading.operation.text.FieldJoiner;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.test.PlatformTest;
import cascading.tuple.Fields;

import static data.InputData.inputFileIps;

@PlatformTest(platforms = {"local", "hadoop"})
public class ParallelCascadeTest extends PlatformTestCase
  {
  public ParallelCascadeTest()
    {
    super( true );
    }

  private Flow firstFlow( String name )
    {
    Tap source = getPlatform().getTextFile( inputFileIps );

    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "ip" ) ), new Fields( "ip" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "ip" ), getOutputPath( name ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow secondFlow( String name, Tap source )
    {
    Pipe pipe = new Pipe( name );

    pipe = new Each( pipe, new RegexSplitter( new Fields( "first", "second", "third", "fourth" ), "\\." ) );
    pipe = new Each( pipe, new FieldJoiner( new Fields( "mangled" ), "-" ) );

    Tap sink = getPlatform().getDelimitedFile( new Fields( "mangled" ), getOutputPath( name ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( source, sink, pipe );
    }

  private Flow thirdFlow( Tap lhs, Tap rhs )
    {
    Pipe lhsPipe = new Pipe( "lhs" );
    Pipe rhsPipe = new Pipe( "rhs" );

    Pipe pipe = new CoGroup( lhsPipe, new Fields( 0 ), rhsPipe, new Fields( 0 ), Fields.size( 2 ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "fourth" ), SinkMode.REPLACE );

    return getPlatform().getFlowConnector().connect( Cascades.tapsMap( Pipe.pipes( lhsPipe, rhsPipe ), Tap.taps( lhs, rhs ) ), sink, pipe );
    }

  public void testCascade() throws IOException
    {
    getPlatform().copyFromLocal( inputFileIps );

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