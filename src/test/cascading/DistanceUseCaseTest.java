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
import java.io.Serializable;

import cascading.assembly.EuclideanDistance;
import cascading.assembly.PearsonDistance;
import cascading.assembly.SortElements;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Sum;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.TextLine;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryIterator;

/** @version $Id: //depot/calku/cascading/src/test/cascading/DistanceUseCaseTest.java#4 $ */
public class DistanceUseCaseTest extends ClusterTestCase implements Serializable
  {
  String inputFileCritics = "build/test/data/critics.txt";

  String outputPathEuclidean = "build/test/output/euclidean/";
  String outputPathPearson = "build/test/output/pearson/";

  public DistanceUseCaseTest()
    {
    super( "distance", false );
    }

  /**
   * Calculate the euclidean distance of the people in the critics.txt file
   *
   * @throws java.io.IOException
   */
  public void testEuclideanDistance() throws Exception
    {
    if( !new File( inputFileCritics ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCritics );

    Tap source = new Hfs( new TextLine(), inputFileCritics );
    Tap sink = new Hfs( new TextLine(), outputPathEuclidean + "/long", true );

    Pipe pipe = new Pipe( "euclidean" );

    // unknown number of elements
    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), new Fields( 0 ), 2 ) );

    // name and rate against others of same movie
    pipe = new CoGroup( pipe, new Fields( "movie" ), 1, new Fields( "name1", "movie", "rate1", "name2", "movie2", "rate2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Fields( "movie", "name1", "rate1", "name2", "rate2" ), new Identity() );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "name1", "rate1" ), new Fields( "name2", "rate2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );
    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // calculate square of diff
    Function sqDiff = new Identity( new Fields( "score" ) )
    {
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry input = functionCall.getArguments();
      functionCall.getOutputCollector().add( new Tuple( Math.pow( input.getTuple().getDouble( 0 ) - input.getTuple().getDouble( 1 ), 2 ) ) );
      }
    };

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Each( pipe, new Fields( "rate1", "rate2" ), sqDiff, Fields.ALL );

    // sum and sqr for each name pair
    pipe = new GroupBy( pipe, new Fields( "name1", "name2" ) );

    Sum distance = new Sum( new Fields( "distance" ) )
    {
    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      Tuple tuple = super.getResult( aggregatorCall );

      aggregatorCall.getOutputCollector().add( new Tuple( 1 / ( 1 + tuple.getDouble( 0 ) ) ) );
      }
    };

    pipe = new Every( pipe, new Fields( "score" ), distance, new Fields( "name1", "name2", "distance" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "graph.dot" );

    flow.complete();

    validateLength( flow, 21 );

    TupleEntryIterator iterator = flow.openSink();
    boolean found = false;

    while( iterator.hasNext() )
      {
      if( iterator.next().get( 1 ).equals( "GeneSeymour\tLisaRose\t0.14814814814814814" ) )
        {
        found = true;
        break;
        }
      }

    assertTrue( "did not calculate score", found );
    }

  /**
   * Calculate the euclidean distance of the people in the critics.txt file
   *
   * @throws java.io.IOException
   */
  public void testEuclideanDistanceShort() throws Exception
    {
    if( !new File( inputFileCritics ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCritics );

    Tap source = new Hfs( new TextLine(), inputFileCritics );
    Tap sink = new Hfs( new TextLine(), outputPathEuclidean + "/short", true );

    // unknown number of elements
    Pipe pipe = new Each( "euclidean", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new CoGroup( pipe, new Fields( "movie" ), 1, new Fields( "name1", "movie", "rate1", "name2", "movie2", "rate2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Fields( "movie", "name1", "rate1", "name2", "rate2" ), new Identity() );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "name1", "rate1" ), new Fields( "name2", "rate2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );

    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // calculate square of diff
    Function sqDiff = new Identity( new Fields( "score" ) )
    {
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry input = functionCall.getArguments();
      functionCall.getOutputCollector().add( new Tuple( Math.pow( input.getTuple().getDouble( 0 ) - input.getTuple().getDouble( 1 ), 2 ) ) );
      }
    };

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Each( pipe, new Fields( "rate1", "rate2" ), sqDiff, Fields.ALL );

    // sum and sqr for each name pair
    pipe = new GroupBy( pipe, new Fields( "name1", "name2" ) );

    Sum distance = new Sum( new Fields( "distance" ) )
    {
    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      Tuple tuple = super.getResult( aggregatorCall );

      aggregatorCall.getOutputCollector().add( new Tuple( 1 / ( 1 + tuple.getDouble( 0 ) ) ) );
      }
    };

    pipe = new Every( pipe, new Fields( "score" ), distance, new Fields( "name1", "name2", "distance" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "graph.dot" );

    flow.complete();

    validateLength( flow, 21 );

    TupleEntryIterator iterator = flow.openSink();
    boolean found = false;

    while( iterator.hasNext() )
      {
      if( iterator.next().get( 1 ).equals( "GeneSeymour\tLisaRose\t0.14814814814814814" ) )
        {
        found = true;
        break;
        }
      }

    assertTrue( "did not calculate score", found );
    }

  public void testEuclideanDistanceComposite() throws Exception
    {
    if( !new File( inputFileCritics ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCritics );

    Tap source = new Hfs( new TextLine(), inputFileCritics );
    Tap sink = new Hfs( new TextLine(), outputPathEuclidean + "/composite", true );

    // unknown number of elements
    Pipe pipe = new Each( "euclidean", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new EuclideanDistance( pipe, new Fields( "name", "movie", "rate" ), new Fields( "name1", "name2", "distance" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "eucdist.dot" );

    flow.complete();

    validateLength( flow, 21 );

    TupleEntryIterator iterator = flow.openSink();
    boolean found = false;

    while( iterator.hasNext() )
      {
      if( iterator.next().get( 1 ).equals( "GeneSeymour\tLisaRose\t0.14814814814814814" ) )
        {
        found = true;
        break;
        }
      }

    assertTrue( "did not calculate score", found );
    }

  public void testPearsonDistanceComposite() throws Exception
    {
    if( !new File( inputFileCritics ).exists() )
      fail( "data file not found" );

    copyFromLocal( inputFileCritics );

    Tap source = new Hfs( new TextLine(), inputFileCritics );
    Tap sink = new Hfs( new TextLine(), outputPathPearson + "/composite", true );

    // unknown number of elements
    Pipe pipe = new Each( "pearson", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new PearsonDistance( pipe, new Fields( "name", "movie", "rate" ), new Fields( "name1", "name2", "distance" ) );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );

//    flow.writeDOT( "peardist.dot" );

    flow.complete();

    validateLength( flow, 21 );

    TupleEntryIterator iterator = flow.openSink();
    boolean found = false;

    while( iterator.hasNext() )
      {
      if( iterator.next().get( 1 ).equals( "GeneSeymour\tLisaRose\t0.39605901719066977" ) )
        {
        found = true;
        break;
        }
      }

    assertTrue( "did not calculate score", found );
    }
  }
