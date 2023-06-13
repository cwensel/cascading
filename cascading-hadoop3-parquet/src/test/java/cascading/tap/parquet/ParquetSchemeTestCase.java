/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
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

package cascading.tap.parquet;

import java.time.temporal.ChronoUnit;

import cascading.CascadingTestCase;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.nested.json.JSONCoercibleType;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.type.InstantType;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

public class ParquetSchemeTestCase extends CascadingTestCase
  {
  @Test
  public void writeRead() throws Exception
    {
    HadoopFlowProcess flowProcess = new HadoopFlowProcess( new JobConf() );
    Fields fields = Fields.NONE
      .append( new Fields( "integer", int.class ) )
      .append( new Fields( "Integer", Integer.class ) )
      .append( new Fields( "long", long.class ) )
      .append( new Fields( "Long", Long.class ) )
      .append( new Fields( "json", JSONCoercibleType.TYPE ) )
      .append( new Fields( "timestampSeconds", InstantType.ISO_SECONDS ) )
      .append( new Fields( "timestampMillis", InstantType.ISO_MILLIS ) )
      .append( new Fields( "timestampMicros", InstantType.ISO_MICROS ) )
      .append( new Fields( "timestampNanos", InstantType.ISO_NANOS ) );

    TypedParquetScheme parquetScheme = new TypedParquetScheme( fields );

    int totalItems = 10000;

    Hfs output = new Hfs( parquetScheme, getOutputPath() );

    try( TupleEntryCollector collector = output.openForWrite( flowProcess ) )
      {
      for( int i = 0; i < totalItems; i++ )
        {
        boolean isEven = i % 2 == 0;
        Tuple tuple = new Tuple(
          i,
          isEven ? Integer.valueOf( i ) : null,
          (long) i,
          isEven ? Long.valueOf( i ) : null,
          isEven ? JSONCoercibleType.TYPE.canonical( "{\"key\":" + i + "}" ) : null,
          InstantType.ISO_SECONDS.canonical( "2023-06-12T21:29:58Z" ).plus( i, ChronoUnit.SECONDS ),
          InstantType.ISO_MILLIS.canonical( "2023-06-12T21:29:58.001Z" ).plus( i, ChronoUnit.MILLIS ),
          InstantType.ISO_MICROS.canonical( "2023-06-12T21:29:58.000001Z" ).plus( i, ChronoUnit.MICROS ),
          InstantType.ISO_NANOS.canonical( "2023-06-12T21:29:58.000000001Z" ).plus( i, ChronoUnit.NANOS )
        );
        collector.add( tuple );
        }
      }

    Hfs input = new Hfs( parquetScheme, getOutputPath() );

    int count = 0;
    try( TupleEntryIterator entryIterator = input.openForRead( flowProcess ) )
      {
      while( entryIterator.hasNext() )
        {
        boolean isEven = count % 2 == 0;

        TupleEntry next = entryIterator.next();
        assertEquals( 9, next.getFields().size() );
        assertEquals( count, next.getInteger( 0 ) );
        assertEquals( isEven ? count : null, next.getObject( 1 ) );
        assertEquals( count, next.getLong( 2 ) );
        assertEquals( isEven ? (long) count : null, next.getObject( 3 ) );
        assertEquals( isEven ? JSONCoercibleType.TYPE.canonical( "{\"key\":" + count + "}" ) : null, next.getObject( 4 ) );
        assertEquals( InstantType.ISO_SECONDS.canonical( "2023-06-12T21:29:58Z" ).plus( count, ChronoUnit.SECONDS ), next.getObject( 5 ) );
        assertEquals( InstantType.ISO_MILLIS.canonical( "2023-06-12T21:29:58.001Z" ).plus( count, ChronoUnit.MILLIS ), next.getObject( 6 ) );
        assertEquals( InstantType.ISO_MICROS.canonical( "2023-06-12T21:29:58.000001Z" ).plus( count, ChronoUnit.MICROS ), next.getObject( 7 ) );
        assertEquals( InstantType.ISO_NANOS.canonical( "2023-06-12T21:29:58.000000001Z" ).plus( count, ChronoUnit.NANOS ), next.getObject( 8 ) );
        count++;
        }
      }

    assertEquals( totalItems, count );
    }
  }
