/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.nested.json;

import java.util.LinkedHashMap;
import java.util.Map;

import cascading.CascadingTestCase;
import cascading.nested.core.NestedAggregate;
import cascading.nested.core.aggregate.AverageDoubleNestedAggregate;
import cascading.nested.core.aggregate.SimpleNestedAggregate;
import cascading.nested.core.aggregate.SumDoubleNestedAggregate;
import cascading.nested.core.aggregate.SumLongNestedAggregate;
import cascading.operation.SerFunction;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleListCollector;
import cascading.tuple.type.CoercibleType;
import com.fasterxml.jackson.databind.JsonNode;
import heretical.pointer.path.Pointer;
import org.junit.Test;

/**
 *
 */
public class JSONGetAllAggregateFunctionTest extends CascadingTestCase
  {
  @Test
  public void testGetAggregateSumLong()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.peopleWithNulls );

    Map<String, NestedAggregate<JsonNode, ?>> pointerMap = new LinkedHashMap<>();

    pointerMap.put( "/person/age", new SumLongNestedAggregate<>( new Fields( "sum", Long.TYPE ) ) );

    JSONGetAllAggregateFunction function = new JSONGetAllAggregateFunction( "/people/*", pointerMap );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 1, result.size() );
    assertEquals( 99L, result.iterator().next().getObject( 0 ) );
    }

  @Test
  public void testGetAggregateSumDouble()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.peopleWithNulls );

    Map<String, NestedAggregate<JsonNode, ?>> pointerMap = new LinkedHashMap<>();

    pointerMap.put( "/person/age", new SumDoubleNestedAggregate<>( new Fields( "sum", Double.class ) ) );

    JSONGetAllAggregateFunction function = new JSONGetAllAggregateFunction( "/people/*", pointerMap );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 1, result.size() );
    assertEquals( 99D, result.iterator().next().getObject( 0 ) );
    }

  @Test
  public void testGetAggregateAverageDoubleAll()
    {
    runAverage( new AverageDoubleNestedAggregate<>( new Fields( "avg", Double.class ) ), 33.0 );
    }

  @Test
  public void testGetAggregateAverageDoubleAllPrimitive()
    {
    runAverage( new AverageDoubleNestedAggregate<>( new Fields( "avg", Double.TYPE ) ), 33.0 );
    }

  @Test
  public void testGetAggregateAverageDoubleNonNull()
    {
    runAverage( new AverageDoubleNestedAggregate<>( new Fields( "avg", Double.class ), AverageDoubleNestedAggregate.Include.NO_NULLS ), 49.5 );
    }

  @Test
  public void testGetAggregateAverageDoubleNonNullPrimitive()
    {
    runAverage( new AverageDoubleNestedAggregate<>( new Fields( "avg", Double.TYPE ), AverageDoubleNestedAggregate.Include.NO_NULLS ), 49.5 );
    }

  private void runAverage( AverageDoubleNestedAggregate<JsonNode> avg, double expected )
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.peopleWithNulls );

    Map<String, NestedAggregate<JsonNode, ?>> pointerMap = new LinkedHashMap<>();

    pointerMap.put( "/person/age", avg );

    JSONGetAllAggregateFunction function = new JSONGetAllAggregateFunction( "/people/*", pointerMap );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 1, result.size() );
    assertEquals( expected, result.iterator().next().getObject( 0 ) );
    }

  @Test
  public void testGetAggregateFunction()
    {
    TupleEntry entry = new TupleEntry( new Fields( "json", JSONCoercibleType.TYPE ), Tuple.size( 1 ) );

    entry.setObject( 0, JSONData.peopleWithNulls );

    Map<String, NestedAggregate<JsonNode, ?>> pointerMap = new LinkedHashMap<>();

    SerFunction<CoercibleType<JsonNode>, SimpleNestedAggregate.AggregateContext<JsonNode>> factory = ( c ) -> new SimpleNestedAggregate.AggregateContext<JsonNode>()
      {
      final Pointer<JsonNode> weightPointer = ( (JSONCoercibleType) c ).getNestedPointerCompiler().compile( "/weight" );
      final Pointer<JsonNode> heightPointer = ( (JSONCoercibleType) c ).getNestedPointerCompiler().compile( "/height" );

      final Tuple result = Tuple.size( 1 );
      int count = 0;
      double sum = 0;

      @Override
      public void aggregate( JsonNode jsonNode )
        {
        long weight = weightPointer.at( jsonNode ).asLong( -1 );
        long height = heightPointer.at( jsonNode ).asLong( -1 );

        if( weight == -1 || height == -1 )
          return;

        count++;
        sum += (double) height / (double) weight;
        }

      @Override
      public Tuple complete()
        {
        result.set( 0, sum / count );
        return result;
        }

      @Override
      public void reset()
        {
        count = 0;
        sum = 0;
        }
      };

    pointerMap.put( "/person/measures", new SimpleNestedAggregate<>( new Fields( "ratio", Long.TYPE ), factory ) );

    JSONGetAllAggregateFunction function = new JSONGetAllAggregateFunction( "/people/*", pointerMap );

    TupleListCollector result = invokeFunction( function, entry, new Fields( "result" ) );

    assertEquals( 1, result.size() );
    assertEquals( 0.55d, result.iterator().next().getObject( 0 ) );
    }
  }
