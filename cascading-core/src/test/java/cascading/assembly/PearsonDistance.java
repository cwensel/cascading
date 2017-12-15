/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Computes the pearson distance between every unique set of first fields, with using the label and value of each element.
 * <p>
 * Expects on input three values: item, label, value
 */
public class PearsonDistance extends CrossTab
  {
  public PearsonDistance( Pipe previous )
    {
    this( previous, Fields.size( 3 ), new Fields( "n1", "n2", "pearson" ) );
    }

  public PearsonDistance( Pipe previous, Fields argumentFieldSelector, Fields fieldDeclaration )
    {
    super( previous, argumentFieldSelector, new Pearson(), fieldDeclaration );
    }

  private static class Pearson extends CrossTabOperation<Map<String, Double>>
    {
    private static final String COUNT = "count";
    private static final String SUM1 = "sum1";
    private static final String SUM2 = "sum2";
    private static final String SUMSQRS1 = "sumsqrs1";
    private static final String SUMSQRS2 = "sumsqrs2";
    private static final String SUMPROD = "sumprod";

    public Pearson()
      {
      super( new Fields( "pearson" ) );
      }

    public void start( FlowProcess flowProcess, AggregatorCall<Map<String, Double>> aggregatorCall )
      {
      if( aggregatorCall.getContext() == null )
        aggregatorCall.setContext( new HashMap<String, Double>() );

      Map<String, Double> context = aggregatorCall.getContext();

      context.put( COUNT, 0d );
      context.put( SUM1, 0d );
      context.put( SUM2, 0d );
      context.put( SUMSQRS1, 0d );
      context.put( SUMSQRS2, 0d );
      context.put( SUMPROD, 0d );
      }

    public void aggregate( FlowProcess flowProcess, AggregatorCall<Map<String, Double>> aggregatorCall )
      {
      Map<String, Double> context = aggregatorCall.getContext();
      TupleEntry entry = aggregatorCall.getArguments();

      context.put( COUNT, ( (Double) context.get( COUNT ) ) + 1d );

      context.put( SUM1, ( (Double) context.get( SUM1 ) ) + entry.getTuple().getDouble( 0 ) );
      context.put( SUM2, ( (Double) context.get( SUM2 ) ) + entry.getTuple().getDouble( 1 ) );

      context.put( SUMSQRS1, ( (Double) context.get( SUMSQRS1 ) ) + Math.pow( entry.getTuple().getDouble( 0 ), 2 ) );
      context.put( SUMSQRS2, ( (Double) context.get( SUMSQRS2 ) ) + Math.pow( entry.getTuple().getDouble( 1 ), 2 ) );

      context.put( SUMPROD, ( (Double) context.get( SUMPROD ) ) + ( entry.getTuple().getDouble( 0 ) * entry.getTuple().getDouble( 1 ) ) );
      }

    public void complete( FlowProcess flowProcess, AggregatorCall<Map<String, Double>> aggregatorCall )
      {
      Map<String, Double> context = aggregatorCall.getContext();
      Double count = (Double) context.get( COUNT );
      Double sum1 = (Double) context.get( SUM1 );
      Double sum2 = (Double) context.get( SUM2 );

      double num = (Double) context.get( SUMPROD ) - ( sum1 * sum2 / count );
      double den = Math.sqrt( ( (Double) context.get( SUMSQRS1 ) - Math.pow( sum1, 2 ) / count ) * ( (Double) context.get( SUMSQRS2 ) - Math.pow( sum2, 2 ) / count ) );

      if( den == 0 )
        aggregatorCall.getOutputCollector().add( new Tuple( 0 ) );
      else
        aggregatorCall.getOutputCollector().add( new Tuple( num / den ) );
      }
    }
  }
