/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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
import java.util.List;

import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

/**
 * Class TupleOutputStream is used internally to write Tuples to storage.
 */
public class TupleOutputStream extends DataOutputStream
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TupleInputStream.class );

  /** Field WRITABLE_TOKEN  */
  public static final int WRITABLE_TOKEN = 32;

  /** Field elementWriter  */
  ElementWriter elementWriter;

  public static interface ElementWriter
    {
    void write( DataOutputStream outputStream, Comparable comparable ) throws IOException;

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
    List<Comparable> elements = Tuple.elements( tuple );

    WritableUtils.writeVInt( this, elements.size() );

    for( Object element : elements )
      {
      if( element == null )
        {
        WritableUtils.writeVInt( this, 0 );
        continue;
        }

      Class type = element.getClass();

      if( String.class == type )
        {
        WritableUtils.writeVInt( this, 1 );
        WritableUtils.writeString( this, (String) element );
        }
      else if( Float.class == type )
        {
        WritableUtils.writeVInt( this, 2 );
        writeFloat( (Float) element );
        }
      else if( Double.class == type )
        {
        WritableUtils.writeVInt( this, 3 );
        writeDouble( (Double) element );
        }
      else if( Integer.class == type )
        {
        WritableUtils.writeVInt( this, 4 );
        WritableUtils.writeVInt( this, (Integer) element );
        }
      else if( Long.class == type )
        {
        WritableUtils.writeVInt( this, 5 );
        WritableUtils.writeVLong( this, (Long) element );
        }
      else if( Boolean.class == type )
        {
        WritableUtils.writeVInt( this, 6 );
        writeBoolean( (Boolean) element );
        }
      else if( Short.class == type )
        {
        WritableUtils.writeVInt( this, 7 );
        writeShort( (Short) element );
        }
      else if( Tuple.class == type )
        {
        WritableUtils.writeVInt( this, 8 );
        writeTuple( (Tuple) element );
        }
      else if( TuplePair.class == type )
        {
        WritableUtils.writeVInt( this, 9 );
        writeTuplePair( (TuplePair) element );
        }
      else if( IndexTuple.class == type )
        {
        WritableUtils.writeVInt( this, 10 );
        writeIndexTuple( (IndexTuple) element );
        }
      else if( element instanceof Comparable )
        {
        elementWriter.write( this, (Comparable) element );
        }
      else
        {
        throw new IOException( "could not write unknown element type: " + element.getClass().getName() );
        }
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
