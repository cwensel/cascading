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

package cascading.tuple;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class TupleOutputStream is used internally to write Tuples to storage. */
public abstract class TupleOutputStream extends DataOutputStream
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( TupleOutputStream.class );

  protected interface TupleElementWriter
    {
    void write( TupleOutputStream stream, Object element ) throws IOException;
    }

  private final Map<Class, TupleElementWriter> tupleElementWriters;
  /** Field elementWriter */
  final ElementWriter elementWriter;

  public interface ElementWriter
    {
    void write( DataOutputStream outputStream, Object object ) throws IOException;

    void close();
    }

  public TupleOutputStream( Map<Class, TupleElementWriter> tupleElementWriters, OutputStream outputStream, ElementWriter elementWriter )
    {
    super( outputStream );
    this.tupleElementWriters = tupleElementWriters;
    this.elementWriter = elementWriter;
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

  public abstract void writeIndexTuple( IndexTuple indexTuple ) throws IOException;

  /**
   * Method write is used by Hadoop to write this Tuple instance out to a file.
   *
   * @throws java.io.IOException when
   */
  private void write( Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    writeNumElements( tuple );

    for( Object element : elements )
      {
      if( element == null )
        {
        writeNull();
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

  protected abstract void writeNull() throws IOException;

  protected abstract void writeNumElements( Tuple tuple ) throws IOException;

  @Override
  public void close() throws IOException
    {
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
