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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

/**
 * Class TupleInputStream is used internally to read Tuples from storage.
 */
public class TupleInputStream extends DataInputStream
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( TupleInputStream.class );

  ElementReader elementReader;

  public static interface ElementReader
    {
    Comparable read( int token, DataInputStream inputStream ) throws IOException;

    void close();
    }

  public TupleInputStream( InputStream inputStream, ElementReader elementReader )
    {
    super( inputStream );
    this.elementReader = elementReader;
    }

  public TupleInputStream( InputStream inputStream )
    {
    super( inputStream );
    this.elementReader = new TupleSerialization().getElementReader();
    }

  public Tuple readTuple() throws IOException
    {
    return readTuple( new Tuple() );
    }

  public Tuple readTuple( Tuple tuple ) throws IOException
    {
    List<Comparable> elements = Tuple.elements( tuple );

    elements.clear();
    int len = WritableUtils.readVInt( this );

    for( int i = 0; i < len; i++ )
      elements.add( readType( WritableUtils.readVInt( this ) ) );

    return tuple;
    }

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

  public IndexTuple readIndexTuple( IndexTuple indexTuple ) throws IOException
    {
    indexTuple.setIndex( WritableUtils.readVInt( this ) );
    indexTuple.setTuple( readTuple() );

    return indexTuple;
    }

  private final Comparable readType( int type ) throws IOException
    {
    switch( type )
      {
      case 0:
        return null;
      case 1:
        return WritableUtils.readString( this );
      case 2:
        return readFloat();
      case 3:
        return readDouble();
      case 4:
        return WritableUtils.readVInt( this );
      case 5:
        return WritableUtils.readVLong( this );
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
