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

package cascading.tap.parquet.convert;

import cascading.tuple.Tuple;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

public class TupleConverter extends GroupConverter
  {

  protected Tuple currentTuple;
  private final Converter[] converters;

  public TupleConverter( GroupType parquetSchema )
    {
    int schemaSize = parquetSchema.getFieldCount();

    this.converters = new Converter[ schemaSize ];
    for( int i = 0; i < schemaSize; i++ )
      {
      Type type = parquetSchema.getType( i );
      converters[ i ] = newConverter( type, i );
      }
    }

  private Converter newConverter( Type type, int i )
    {
    if( !type.isPrimitive() )
      {
      throw new IllegalArgumentException( "cascading can only build tuples from primitive types" );
      }
    else
      {
      return new TuplePrimitiveConverter( this, i );
      }
    }

  @Override
  public Converter getConverter( int fieldIndex )
    {
    return converters[ fieldIndex ];
    }

  @Override
  final public void start()
    {
    currentTuple = Tuple.size( converters.length );
    }

  @Override
  public void end()
    {
    }

  final public Tuple getCurrentTuple()
    {
    return currentTuple;
    }

  static final class TuplePrimitiveConverter extends PrimitiveConverter
    {
    private final TupleConverter parent;
    private final int index;

    public TuplePrimitiveConverter( TupleConverter parent, int index )
      {
      this.parent = parent;
      this.index = index;
      }

    @Override
    public void addBinary( Binary value )
      {
      parent.getCurrentTuple().setString( index, value.toStringUsingUTF8() );
      }

    @Override
    public void addBoolean( boolean value )
      {
      parent.getCurrentTuple().setBoolean( index, value );
      }

    @Override
    public void addDouble( double value )
      {
      parent.getCurrentTuple().setDouble( index, value );
      }

    @Override
    public void addFloat( float value )
      {
      parent.getCurrentTuple().setFloat( index, value );
      }

    @Override
    public void addInt( int value )
      {
      parent.getCurrentTuple().setInteger( index, value );
      }

    @Override
    public void addLong( long value )
      {
      parent.getCurrentTuple().setLong( index, value );
      }
    }
  }
