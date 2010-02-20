/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

/** Class TupleOutputStream is used internally to write Tuples to storage. */
public class TupleOutputStream extends DataOutputStream
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TupleInputStream.class );

  /** Field WRITABLE_TOKEN */
  public static final int WRITABLE_TOKEN = 32;

  private interface TupleElementWriter
    {
    void write( TupleOutputStream stream, Object element ) throws IOException;
    }

  private static Map<Class, TupleElementWriter> tupleElementWriters = new IdentityHashMap<Class, TupleElementWriter>();

  static
    {
    tupleElementWriters.put( String.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 1 );
      WritableUtils.writeString( stream, (String) element );
      }
    } );

    tupleElementWriters.put( Float.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 2 );
      stream.writeFloat( (Float) element );
      }
    } );

    tupleElementWriters.put( Double.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 3 );
      stream.writeDouble( (Double) element );
      }
    } );

    tupleElementWriters.put( Integer.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 4 );
      WritableUtils.writeVInt( stream, (Integer) element );
      }
    } );

    tupleElementWriters.put( Long.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 5 );
      WritableUtils.writeVLong( stream, (Long) element );
      }
    } );

    tupleElementWriters.put( Boolean.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 6 );
      stream.writeBoolean( (Boolean) element );
      }
    } );

    tupleElementWriters.put( Short.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 7 );
      stream.writeShort( (Short) element );
      }
    } );

    tupleElementWriters.put( Tuple.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 8 );
      stream.writeTuple( (Tuple) element );
      }
    } );

    tupleElementWriters.put( TuplePair.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 9 );
      stream.writeTuplePair( (TuplePair) element );
      }
    } );

    tupleElementWriters.put( IndexTuple.class, new TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      WritableUtils.writeVInt( stream, 10 );
      stream.writeIndexTuple( (IndexTuple) element );
      }
    } );

    }

  /** Field elementWriter */
  ElementWriter elementWriter;

  public interface ElementWriter
    {
    void write( DataOutputStream outputStream, Object object ) throws IOException;

    void close();
    }

  public TupleOutputStream( OutputStream outputStream, ElementWriter elementWriter )
    {
    super( outputStream );
    this.elementWriter = elementWriter;
    }

  public TupleOutputStream( OutputStream outputStream )
    {
    super( outputStream );
    this.elementWriter = new TupleSerialization().getElementWriter();
    }

  public void writeTuple( Tuple tuple ) throws IOException
    {
    write( tuple );
    }

  public void writeTuplePair( TuplePair tuplePair ) throws IOException
    {
    Tuple[] tuples = TuplePair.tuples( tuplePair );

    write( tuples[ 0 ] );
    write( tuples[ 1 ] );
    }

  public void writeIndexTuple( IndexTuple indexTuple ) throws IOException
    {
    WritableUtils.writeVInt( this, indexTuple.getIndex() );
    writeTuple( indexTuple.getTuple() );
    }

  /**
   * Method write is used by Hadoop to write this Tuple instance out to a file.
   *
   * @throws java.io.IOException when
   */
  private void write( Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    WritableUtils.writeVInt( this, elements.size() );

    for( Object element : elements )
      {
      if( element == null )
        {
        WritableUtils.writeVInt( this, 0 );
        continue;
        }

      Class type = element.getClass();
      TupleElementWriter tupleElementWriter = tupleElementWriters.get( type );

      if( tupleElementWriter != null )
        tupleElementWriter.write( this, element );
      else
        elementWriter.write( this, element );
      }
    }

  @Override
  public void close() throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "closing tuple output stream" );

    try
      {
      super.close();
      }
    finally
      {
      if( elementWriter != null )
        elementWriter.close();
      }
    }
  }
