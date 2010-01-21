/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.assembly;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Computes the euclidean distance between every unique set of first fields,
 * with using the label and value of each element.
 * <p/>
 * Expects on input three values: item, label, value
 */
public class EuclideanDistance extends CrossTab
  {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor
   *
   * @param previous the upstream pipe
   */
  public EuclideanDistance( Pipe previous )
    {
    this( previous, Fields.size( 3 ), new Fields( "n1", "n2", "euclidean" ) );
    }

  /**
   * Constructor
   *
   * @param previous
   * @param argumentFieldSelector
   * @param fieldDeclaration
   */
  public EuclideanDistance( Pipe previous, Fields argumentFieldSelector, Fields fieldDeclaration )
    {
    super( previous, argumentFieldSelector, new Euclidean(), fieldDeclaration );
    }

  /** TODO: doc me */
  protected static class Euclidean extends CrossTabOperation<Double[]>
    {
    private static final long serialVersionUID = 1L;

    public Euclidean()
      {
      super( new Fields( "euclidean" ) );
      }

    public void start( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
      {
      aggregatorCall.setContext( new Double[]{0d} );
      }

    public void aggregate( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
      {
      TupleEntry entry = aggregatorCall.getArguments();
      aggregatorCall.getContext()[ 0 ] += Math.pow( entry.getDouble( 0 ) - entry.getDouble( 1 ), 2 );
      }

    public void complete( FlowProcess flowProcess, AggregatorCall<Double[]> aggregatorCall )
      {
      aggregatorCall.getOutputCollector().add( new Tuple( 1 / ( 1 + aggregatorCall.getContext()[ 0 ] ) ) );
      }
    }
  }
