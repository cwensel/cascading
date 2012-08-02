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

package cascading.tuple.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.List;

import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class TupleInputStream is used internally to read Tuples from storage. */
public abstract class TupleInputStream extends DataInputStream
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( TupleInputStream.class );

  /** Field inputStream */
  protected final InputStream inputStream;
  /** Field elementReader */
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
