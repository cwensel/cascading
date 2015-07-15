/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

    staticTupleTypedElementReaders.put( Void.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return null;
      }
    } );

    staticTupleTypedElementReaders.put( String.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readString();
      }
    } );

    staticTupleTypedElementReaders.put( Float.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullFloat();
      }
    } );

    staticTupleTypedElementReaders.put( Double.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullDouble();
      }
    } );

    staticTupleTypedElementReaders.put( Integer.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullVInt();
      }
    } );

    staticTupleTypedElementReaders.put( Long.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullVLong();
      }
    } );

    staticTupleTypedElementReaders.put( Boolean.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullBoolean();
      }
    } );

    staticTupleTypedElementReaders.put( Short.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readNullShort();
      }
    } );

    staticTupleTypedElementReaders.put( Float.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readFloat();
      }
    } );

    staticTupleTypedElementReaders.put( Double.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readDouble();
      }
    } );

    staticTupleTypedElementReaders.put( Integer.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readVInt();
      }
    } );

    staticTupleTypedElementReaders.put( Long.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readVLong();
      }
    } );

    staticTupleTypedElementReaders.put( Boolean.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readBoolean();
      }
    } );

    staticTupleTypedElementReaders.put( Short.TYPE, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readShort();
      }
    } );

    staticTupleTypedElementReaders.put( Tuple.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readTuple();
      }
    } );

    staticTupleTypedElementReaders.put( TuplePair.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readTuplePair();
      }
    } );

    staticTupleTypedElementReaders.put( IndexTuple.class, new TupleElementReader<HadoopTupleInputStream>()
    {
    @Override
    public Object read( HadoopTupleInputStream stream ) throws IOException
      {
      return stream.readIndexTuple();
      }
    } );
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
        readers[ i ] = new TupleElementReader()
        {
        @Override
        public Object read( TupleInputStream stream ) throws IOException
          {
          return elementReader.read( classes[ index ], stream );
          }
        };
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
