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

import java.io.IOException;
import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.Map;

import cascading.tuple.Tuple;
import cascading.tuple.io.IndexTuple;
import cascading.tuple.io.TupleOutputStream;
import cascading.tuple.io.TuplePair;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class HadoopTupleOutputStream extends TupleOutputStream
  {
  /** Field WRITABLE_TOKEN */
  public static final int WRITABLE_TOKEN = 32;

  private static final Map<Class, TupleElementWriter> staticTupleUnTypedElementWriters = new IdentityHashMap<>();
  private static final Map<Class, TupleElementWriter> staticTupleTypedElementWriters = new IdentityHashMap<>();

  static
    {
    // untyped

    staticTupleUnTypedElementWriters.put( String.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 1 );
    WritableUtils.writeString( stream, (String) element );
    } );

    staticTupleUnTypedElementWriters.put( Float.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 2 );
    stream.writeFloat( (Float) element );
    } );

    staticTupleUnTypedElementWriters.put( Double.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 3 );
    stream.writeDouble( (Double) element );
    } );

    staticTupleUnTypedElementWriters.put( Integer.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 4 );
    WritableUtils.writeVInt( stream, (Integer) element );
    } );

    staticTupleUnTypedElementWriters.put( Long.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 5 );
    WritableUtils.writeVLong( stream, (Long) element );
    } );

    staticTupleUnTypedElementWriters.put( Boolean.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 6 );
    stream.writeBoolean( (Boolean) element );
    } );

    staticTupleUnTypedElementWriters.put( Short.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 7 );
    stream.writeShort( (Short) element );
    } );

    staticTupleUnTypedElementWriters.put( Tuple.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 8 );
    stream.writeTuple( (Tuple) element );
    } );

    staticTupleUnTypedElementWriters.put( TuplePair.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 9 );
    stream.writeTuplePair( (TuplePair) element );
    } );

    staticTupleUnTypedElementWriters.put( IndexTuple.class, ( stream, element ) ->
    {
    WritableUtils.writeVInt( stream, 10 );
    stream.writeIndexTuple( (IndexTuple) element );
    } );

    // typed

    staticTupleTypedElementWriters.put( Void.class, ( stream, element ) ->
    {
    // do nothing
    } );

    staticTupleTypedElementWriters.put( String.class, ( stream, element ) -> WritableUtils.writeString( stream, (String) element ) );

    staticTupleTypedElementWriters.put( Float.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    stream.writeFloat( (Float) element );
    } );

    staticTupleTypedElementWriters.put( Double.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    stream.writeDouble( (Double) element );
    } );

    staticTupleTypedElementWriters.put( Integer.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    WritableUtils.writeVInt( stream, (Integer) element );
    } );

    staticTupleTypedElementWriters.put( Long.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    WritableUtils.writeVLong( stream, (Long) element );
    } );

    staticTupleTypedElementWriters.put( Boolean.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    stream.writeBoolean( (Boolean) element );
    } );

    staticTupleTypedElementWriters.put( Short.class, ( stream, element ) ->
    {
    if( element == null )
      {
      stream.writeByte( 0 );
      return;
      }

    stream.writeByte( 1 );
    stream.writeShort( (Short) element );
    } );

    staticTupleTypedElementWriters.put( Float.TYPE, ( stream, element ) ->
    {
    if( element == null )
      stream.writeFloat( 0 );
    else
      stream.writeFloat( (Float) element );
    } );

    staticTupleTypedElementWriters.put( Double.TYPE, ( stream, element ) ->
    {
    if( element == null )
      stream.writeDouble( 0 );
    else
      stream.writeDouble( (Double) element );
    } );

    staticTupleTypedElementWriters.put( Integer.TYPE, ( stream, element ) ->
    {
    if( element == null )
      WritableUtils.writeVInt( stream, 0 );
    else
      WritableUtils.writeVInt( stream, (Integer) element );
    } );

    staticTupleTypedElementWriters.put( Long.TYPE, ( stream, element ) ->
    {
    if( element == null )
      WritableUtils.writeVLong( stream, 0 );
    else
      WritableUtils.writeVLong( stream, (Long) element );
    } );

    staticTupleTypedElementWriters.put( Boolean.TYPE, ( stream, element ) ->
    {
    if( element == null )
      stream.writeBoolean( false );
    else
      stream.writeBoolean( (Boolean) element );
    } );

    staticTupleTypedElementWriters.put( Short.TYPE, ( stream, element ) ->
    {
    if( element == null )
      stream.writeShort( 0 );
    else
      stream.writeShort( (Short) element );
    } );

    staticTupleTypedElementWriters.put( Tuple.class, ( stream, element ) -> stream.writeTuple( (Tuple) element ) );

    staticTupleTypedElementWriters.put( TuplePair.class, ( stream, element ) -> stream.writeTuplePair( (TuplePair) element ) );

    staticTupleTypedElementWriters.put( IndexTuple.class, ( stream, element ) -> stream.writeIndexTuple( (IndexTuple) element ) );
    }

  public static TupleElementWriter[] getWritersFor( final ElementWriter elementWriter, final Class[] keyClasses )
    {
    if( keyClasses == null || keyClasses.length == 0 )
      return null;

    TupleElementWriter[] writers = new TupleElementWriter[ keyClasses.length ];

    for( int i = 0; i < keyClasses.length; i++ )
      {
      TupleElementWriter writer = staticTupleTypedElementWriters.get( keyClasses[ i ] );

      if( writer != null )
        {
        writers[ i ] = writer;
        }
      else
        {
        final int index = i;
        writers[ i ] = ( stream, element ) -> elementWriter.write( stream, keyClasses[ index ], element );
        }
      }

    return writers;
    }

  public HadoopTupleOutputStream( OutputStream outputStream, ElementWriter elementWriter )
    {
    super( staticTupleUnTypedElementWriters, staticTupleTypedElementWriters, outputStream, elementWriter );
    }

  @Override
  protected void writeIntInternal( int value ) throws IOException
    {
    WritableUtils.writeVInt( this, value );
    }

  public void writeIndexTuple( IndexTuple indexTuple ) throws IOException
    {
    writeIntInternal( indexTuple.getIndex() );
    writeTuple( indexTuple.getTuple() );
    }
  }
