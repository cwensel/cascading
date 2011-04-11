/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.tuple.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.Map;

import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.tuple.TupleOutputStream;
import cascading.tuple.TuplePair;
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

  protected void writeNull() throws IOException
    {
    WritableUtils.writeVInt( this, 0 );
    }

  protected void writeNumElements( Tuple tuple ) throws IOException
    {
    WritableUtils.writeVInt( this, Tuple.elements( tuple ).size() );
    }

  public void writeIndexTuple( IndexTuple indexTuple ) throws IOException
    {
    WritableUtils.writeVInt( this, indexTuple.getIndex() );
    writeTuple( indexTuple.getTuple() );
    }
  }
