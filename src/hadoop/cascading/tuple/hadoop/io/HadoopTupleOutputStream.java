/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

  private static final Map<Class, TupleElementWriter> staticTupleElementWriters = new IdentityHashMap<Class, TupleElementWriter>();

  static
    {
    staticTupleElementWriters.put( String.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 1 );
      WritableUtils.writeString( stream, (String) element );
      }
    } );

    staticTupleElementWriters.put( Float.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 2 );
      stream.writeFloat( (Float) element );
      }
    } );

    staticTupleElementWriters.put( Double.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 3 );
      stream.writeDouble( (Double) element );
      }
    } );

    staticTupleElementWriters.put( Integer.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 4 );
      WritableUtils.writeVInt( stream, (Integer) element );
      }
    } );

    staticTupleElementWriters.put( Long.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 5 );
      WritableUtils.writeVLong( stream, (Long) element );
      }
    } );

    staticTupleElementWriters.put( Boolean.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 6 );
      stream.writeBoolean( (Boolean) element );
      }
    } );

    staticTupleElementWriters.put( Short.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 7 );
      stream.writeShort( (Short) element );
      }
    } );

    staticTupleElementWriters.put( Tuple.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 8 );
      stream.writeTuple( (Tuple) element );
      }
    } );

    staticTupleElementWriters.put( TuplePair.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 9 );
      stream.writeTuplePair( (TuplePair) element );
      }
    } );

    staticTupleElementWriters.put( IndexTuple.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 10 );
      stream.writeIndexTuple( (IndexTuple) element );
      }
    } );
    }

  public HadoopTupleOutputStream( OutputStream outputStream, ElementWriter elementWriter )
    {
    super( staticTupleElementWriters, outputStream, elementWriter );
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
