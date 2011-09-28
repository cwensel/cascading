/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
