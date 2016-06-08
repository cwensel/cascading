/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.tuple.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class TupleOutputStream is used internally to write Tuples to storage. */
public abstract class TupleOutputStream extends DataOutputStream
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( TupleOutputStream.class );

  public interface TupleElementWriter
    {
    void write( TupleOutputStream stream, Object element ) throws IOException;
    }

  public interface ElementWriter
    {
    void write( DataOutputStream outputStream, Object object ) throws IOException;

    void write( DataOutputStream outputStream, Class<?> type, Object object ) throws IOException;

    void close();
    }

  private final Map<Class, TupleElementWriter> tupleUnTypedElementWriters;
  private final Map<Class, TupleElementWriter> tupleTypedElementWriters;

  /** Field elementWriter */
  final ElementWriter elementWriter;

  public TupleOutputStream( Map<Class, TupleElementWriter> tupleUnTypedElementWriters, Map<Class, TupleElementWriter> tupleTypedElementWriters, OutputStream outputStream, ElementWriter elementWriter )
    {
    super( outputStream );
    this.tupleUnTypedElementWriters = tupleUnTypedElementWriters;
    this.tupleTypedElementWriters = tupleTypedElementWriters;
    this.elementWriter = elementWriter;
    }

  public final TupleOutputStream.TupleElementWriter getWriterFor( final Class type ) throws IOException
    {
    TupleOutputStream.TupleElementWriter tupleElementWriter = tupleTypedElementWriters.get( type );

    if( tupleElementWriter != null )
      return tupleElementWriter;

    return new TupleOutputStream.TupleElementWriter()
    {
    @Override
    public void write( TupleOutputStream stream, Object element ) throws IOException
      {
      elementWriter.write( stream, type, element );
      }
    };
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
    writeUnTyped( tuple );
    }

  public void writeWith( TupleElementWriter[] writers, Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    for( int i = 0; i < writers.length; i++ )
      writers[ i ].write( this, elements.get( i ) );
    }

  public void writeTyped( Class[] classes, Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    for( int i = 0; i < classes.length; i++ )
      {
      Class type = classes[ i ];
      writeTypedElement( type, elements.get( i ) );
      }
    }

  public void writeUnTyped( Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    writeIntInternal( elements.size() );

    for( Object element : elements )
      writeElement( element );
    }

  public void writeElementArray( Object[] elements ) throws IOException
    {
    writeIntInternal( elements.length );

    for( Object element : elements )
      writeElement( element );
    }

  public final void writeTypedElement( Class type, Object element ) throws IOException
    {
    TupleElementWriter tupleElementWriter = tupleTypedElementWriters.get( type );

    if( tupleElementWriter != null )
      tupleElementWriter.write( this, element );
    else
      elementWriter.write( this, type, element );
    }

  public final void writeElement( Object element ) throws IOException
    {
    if( element == null )
      {
      writeIntInternal( 0 );
      return;
      }

    Class type = element.getClass();
    TupleElementWriter tupleElementWriter = tupleUnTypedElementWriters.get( type );

    if( tupleElementWriter != null )
      tupleElementWriter.write( this, element );
    else
      elementWriter.write( this, element );
    }

  protected abstract void writeIntInternal( int value ) throws IOException;

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
