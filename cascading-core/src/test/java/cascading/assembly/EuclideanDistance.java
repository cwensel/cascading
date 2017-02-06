/*
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
