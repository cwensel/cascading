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
import java.io.InputStream;

import cascading.tuple.IndexTuple;
import cascading.tuple.TupleInputStream;
import org.apache.hadoop.io.WritableUtils;

/**
 *
 */
public class HadoopTupleInputStream extends TupleInputStream
  {
  public HadoopTupleInputStream( InputStream inputStream, ElementReader elementReader )
    {
    super( inputStream, elementReader );
    }

  public int getNumElements() throws IOException
    {
    return readVInt();
    }

  public int readToken() throws IOException
    {
    return readVInt();
    }

  public Object getNextElement() throws IOException
    {
    return readType( readToken() );
    }

  public IndexTuple readIndexTuple( IndexTuple indexTuple ) throws IOException
    {
    indexTuple.setIndex( readVInt() );
    indexTuple.setTuple( readTuple() );

    return indexTuple;
    }

  public long readVLong() throws IOException
    {
    return WritableUtils.readVLong( this );
    }

  public int readVInt() throws IOException
    {
    return WritableUtils.readVInt( this );
    }

  public String readString() throws IOException
    {
    return WritableUtils.readString( this );
    }

  protected final Object readType( int type ) throws IOException
    {
    switch( type )
      {
      case 0:
        return null;
      case 1:
        return readString();
      case 2:
        return readFloat();
      case 3:
        return readDouble();
      case 4:
        return readVInt();
      case 5:
        return readVLong();
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
  }
