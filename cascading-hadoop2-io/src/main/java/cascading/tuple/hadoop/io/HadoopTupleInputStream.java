/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.tuple.hadoop.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.IdentityHashMap;
import java.util.Map;

import cascading.tuple.Tuple;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.TupleInputStream;
import cascading.tuple.io.TuplePair;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class HadoopTupleInputStream extends TupleInputStream
  {
  private static final Map<Class, TupleElementReader> staticTupleUnTypedElementReaders = new IdentityHashMap<>();
  private static final Map<Class, TupleElementReader> staticTupleTypedElementReaders = new IdentityHashMap<>();

  static
    {
    // typed

    staticTupleTypedElementReaders.put( Void.class, (TupleElementReader<HadoopTupleInputStream>) stream -> null );

    staticTupleTypedElementReaders.put( String.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readString );

    staticTupleTypedElementReaders.put( Float.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullFloat );

    staticTupleTypedElementReaders.put( Double.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullDouble );

    staticTupleTypedElementReaders.put( Integer.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullVInt );

    staticTupleTypedElementReaders.put( Long.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullVLong );

    staticTupleTypedElementReaders.put( Boolean.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullBoolean );

    staticTupleTypedElementReaders.put( Short.class, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readNullShort );

    staticTupleTypedElementReaders.put( Float.TYPE, (TupleElementReader<HadoopTupleInputStream>) DataInputStream::readFloat );

    staticTupleTypedElementReaders.put( Double.TYPE, (TupleElementReader<HadoopTupleInputStream>) DataInputStream::readDouble );

    staticTupleTypedElementReaders.put( Integer.TYPE, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readVInt );

    staticTupleTypedElementReaders.put( Long.TYPE, (TupleElementReader<HadoopTupleInputStream>) HadoopTupleInputStream::readVLong );

    staticTupleTypedElementReaders.put( Boolean.TYPE, (TupleElementReader<HadoopTupleInputStream>) DataInputStream::readBoolean );

    staticTupleTypedElementReaders.put( Short.TYPE, (TupleElementReader<HadoopTupleInputStream>) DataInputStream::readShort );

    staticTupleTypedElementReaders.put( Tuple.class, (TupleElementReader<HadoopTupleInputStream>) TupleInputStream::readTuple );

    staticTupleTypedElementReaders.put( TuplePair.class, (TupleElementReader<HadoopTupleInputStream>) TupleInputStream::readTuplePair );

    staticTupleTypedElementReaders.put( IndexTuple.class, (TupleElementReader<HadoopTupleInputStream>) TupleInputStream::readIndexTuple );
    }

  public static TupleElementReader[] getReadersFor( final ElementReader elementReader, final Class[] classes )
    {
    if( classes == null || classes.length == 0 )
      return null;

    TupleElementReader[] readers = new TupleElementReader[ classes.length ];

    for( int i = 0; i < classes.length; i++ )
      {
      TupleElementReader reader = staticTupleTypedElementReaders.get( classes[ i ] );

      if( reader != null )
        {
        readers[ i ] = reader;
        }
      else
        {
        final int index = i;
        readers[ i ] = stream -> elementReader.read( classes[ index ], stream );
        }
      }

    return readers;
    }

  public HadoopTupleInputStream( InputStream inputStream, ElementReader elementReader )
    {
    super( inputStream, elementReader );
    }

  public int getNumElements() throws IOException
    {
    return readVInt();
    }

  public int readToken() throws IOException
    {
    return readVInt();
    }

  public Object getNextElement() throws IOException
    {
    return readType( readToken() );
    }

  public IndexTuple readIndexTuple( IndexTuple tuple ) throws IOException
    {
    tuple.setIndex( readVInt() );
    tuple.setTuple( readTuple() );

    return tuple;
    }

  public Long readNullVLong() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return WritableUtils.readVLong( this );
    }

  public long readVLong() throws IOException
    {
    return WritableUtils.readVLong( this );
    }

  public Integer readNullVInt() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return WritableUtils.readVInt( this );
    }

  public int readVInt() throws IOException
    {
    return WritableUtils.readVInt( this );
    }

  public String readString() throws IOException
    {
    return WritableUtils.readString( this );
    }

  private Short readNullShort() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return readShort();
    }

  private Object readNullBoolean() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return readBoolean();
    }

  private Object readNullDouble() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return readDouble();
    }

  private Object readNullFloat() throws IOException
    {
    byte b = this.readByte();

    if( b == 0 )
      return null;

    return readFloat();
    }

  protected final Object readType( int type ) throws IOException
    {
    switch( type )
      {
      case 0:
        return null;
      case 1:
        return readString();
      case 2:
        return readFloat();
      case 3:
        return readDouble();
      case 4:
        return readVInt();
      case 5:
        return readVLong();
      case 6:
        return readBoolean();
      case 7:
        return readShort();
      case 8:
        return readTuple();
      case 9:
        return readTuplePair();
      case 10:
        return readIndexTuple();
      default:
        return elementReader.read( type, this );
      }
    }

  public final Object readType( Class type ) throws IOException
    {
    if( type == Void.class )
      return null;

    if( type == String.class )
      return readString();

    if( type == Float.class )
      return readNullFloat();
    if( type == Double.class )
      return readNullDouble();
    if( type == Integer.class )
      return readNullVInt();
    if( type == Long.class )
      return readNullVLong();
    if( type == Boolean.class )
      return readNullBoolean();
    if( type == Short.class )
      return readNullShort();

    if( type == Float.TYPE )
      return readFloat();
    if( type == Double.TYPE )
      return readDouble();
    if( type == Integer.TYPE )
      return readVInt();
    if( type == Long.TYPE )
      return readVLong();
    if( type == Boolean.TYPE )
      return readBoolean();
    if( type == Short.TYPE )
      return readShort();

    if( type == Tuple.class )
      return readTuple();
    if( type == TuplePair.class )
      return readTuplePair();
    if( type == IndexTuple.class )
      return readIndexTuple();
    else
      return elementReader.read( type, this );
    }
  }
