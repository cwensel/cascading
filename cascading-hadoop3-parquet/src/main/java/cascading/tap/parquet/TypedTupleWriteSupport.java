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

import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class TypedTupleWriteSupport extends WriteSupport<TupleEntry>
  {
  private RecordConsumer recordConsumer;
  public static final String PARQUET_CASCADING_SCHEMA = "parquet.cascading.schema";
  protected Consumer<TupleEntry>[] writers;

  @Override
  public String getName()
    {
    return "cascading";
    }

  @Override
  public WriteContext init( Configuration configuration )
    {
    String schema = configuration.get( PARQUET_CASCADING_SCHEMA );
    MessageType rootSchema = MessageTypeParser.parseMessageType( schema );

    List<Type> schemaFields = rootSchema.getFields();

    writers = new Consumer[ schemaFields.size() ];

    for( int i = 0; i < schemaFields.size(); i++ )
      {
      Type field = schemaFields.get( i );

      if( !field.isPrimitive() )
          throw new UnsupportedOperationException( "complex type not implemented" );

      String fieldName = field.getName();
      PrimitiveType primitiveType = field.asPrimitiveType();

      int index = i;

      switch( primitiveType.getPrimitiveTypeName() )
        {
        case BINARY:
          // the string version of an object (json) may return as null after coercion
          writers[ i ] = t -> writeFieldValue( fieldName, index, t.getString( index ), ( r, v ) -> r.addBinary( Binary.fromString( v ) ) );
          break;
        case BOOLEAN:
        {
        Class<Boolean> type = field.getRepetition() == Type.Repetition.REQUIRED ? Boolean.TYPE : Boolean.class;
        writers[ i ] = t -> writeFieldValue( fieldName, index, (Boolean) t.getObject( index, type ), RecordConsumer::addBoolean );
        }
        break;
        case INT32:
        {
        Class<Integer> type = field.getRepetition() == Type.Repetition.REQUIRED ? Integer.TYPE : Integer.class;
        writers[ i ] = t -> writeFieldValue( fieldName, index, (Integer) t.getObject( index, type ), RecordConsumer::addInteger );
        }
        break;
        case INT64:
        {
        Class<Long> type = field.getRepetition() == Type.Repetition.REQUIRED ? Long.TYPE : Long.class;
        writers[ i ] = t -> writeFieldValue( fieldName, index, (Long) t.getObject( index, type ), RecordConsumer::addLong );
        }
        break;
        case DOUBLE:
        {
        Class<Double> type = field.getRepetition() == Type.Repetition.REQUIRED ? Double.TYPE : Double.class;
        writers[ i ] = t -> writeFieldValue( fieldName, index, (Double) t.getObject( index, type ), RecordConsumer::addDouble );
        }
        break;
        case FLOAT:
        {
        Class<Float> type = field.getRepetition() == Type.Repetition.REQUIRED ? Float.TYPE : Float.class;
        writers[ i ] = t -> writeFieldValue( fieldName, index, (Float) t.getObject( index, type ), RecordConsumer::addFloat );
        }
        break;
        case FIXED_LEN_BYTE_ARRAY:
          throw new UnsupportedOperationException( "Fixed len byte array type not implemented" );
        case INT96:
          throw new UnsupportedOperationException( "Int96 type not implemented" );
        default:
          throw new UnsupportedOperationException( primitiveType.getName() + " type not implemented" );
        }
      }

    return new WriteContext( rootSchema, new HashMap<>() );
    }

  @Override
  public void prepareForWrite( RecordConsumer recordConsumer )
    {
    this.recordConsumer = recordConsumer;
    }

  @Override
  public void write( TupleEntry record )
    {
    if( record == null )
        return;

    recordConsumer.startMessage();

    for( Consumer<TupleEntry> writer : writers )
        writer.accept( record );

    recordConsumer.endMessage();
    }

  protected <V> void writeFieldValue( String name, int index, V value, BiConsumer<RecordConsumer, V> consumer )
    {
    if( value == null )
        return;

    recordConsumer.startField( name, index );
    consumer.accept( recordConsumer, value );
    recordConsumer.endField( name, index );
    }
  }
