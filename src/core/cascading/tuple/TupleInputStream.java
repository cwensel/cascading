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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

/** Class TupleInputStream is used internally to read Tuples from storage. */
public abstract class TupleInputStream extends DataInputStream
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TupleInputStream.class );

  protected final InputStream inputStream;
  protected final ElementReader elementReader;

  public interface ElementReader
    {
    Object read( int token, DataInputStream inputStream ) throws IOException;

    Comparator getComparatorFor( int type, DataInputStream inputStream ) throws IOException;

    void close();
    }

  public TupleInputStream( InputStream inputStream, ElementReader elementReader )
    {
    super( inputStream );
    this.inputStream = inputStream;
    this.elementReader = elementReader;
    }

  public InputStream getInputStream()
    {
    return inputStream;
    }

  public Tuple readTuple() throws IOException
    {
    return readTuple( new Tuple() );
    }

  public Tuple readTuple( Tuple tuple ) throws IOException
    {
    List<Object> elements = Tuple.elements( tuple );

    elements.clear();
    int len = getNumElements();

    for( int i = 0; i < len; i++ )
      elements.add( getNextElement() );

    return tuple;
    }

  public abstract int getNumElements() throws IOException;

  public abstract int readToken() throws IOException;

  public abstract Object getNextElement() throws IOException;

  public TuplePair readTuplePair() throws IOException
    {
    return readTuplePair( new TuplePair() );
    }

  public TuplePair readTuplePair( TuplePair tuplePair ) throws IOException
    {
    Tuple[] tuples = TuplePair.tuples( tuplePair );

    readTuple( tuples[ 0 ] ); // guaranteed to not be null
    readTuple( tuples[ 1 ] ); // guaranteed to not be null

    return tuplePair;
    }

  public IndexTuple readIndexTuple() throws IOException
    {
    return readIndexTuple( new IndexTuple() );
    }

  public abstract IndexTuple readIndexTuple( IndexTuple indexTuple ) throws IOException;

  protected abstract Object readType( int type ) throws IOException;

  public Comparator getComparatorFor( int type ) throws IOException
    {
    if( type >= 0 && type <= 10 )
      return null;

    return elementReader.getComparatorFor( type, this );
    }

  @Override
  public void close() throws IOException
    {
    if( LOG.isDebugEnabled() )
      LOG.debug( "closing tuple input stream" );

    try
      {
      super.close();
      }
    finally
      {
      if( elementReader != null )
        elementReader.close();
      }
    }
  }
