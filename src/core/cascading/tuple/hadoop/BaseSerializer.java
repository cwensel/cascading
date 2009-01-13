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

package cascading.tuple.hadoop;

import java.io.IOException;
import java.io.OutputStream;

import cascading.tuple.TupleOutputStream;
import org.apache.hadoop.io.serializer.Serializer;

abstract class BaseSerializer<T> implements Serializer<T>
  {
  TupleOutputStream outputStream;
  private SerializationElementWriter elementWriter;

  protected BaseSerializer( SerializationElementWriter elementWriter )
    {
    this.elementWriter = elementWriter;
    }

  public void open( OutputStream out )
    {
    if( out instanceof TupleOutputStream )
      outputStream = (TupleOutputStream) out;
    else
      outputStream = new TupleOutputStream( out, elementWriter );
    }

  public void close() throws IOException
    {
    try
      {
      outputStream.close();
      }
    finally
      {
      outputStream = null;
      }
    }
  }
