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

package cascading.tap.local;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.TupleEntrySchemeCollector;

/**
 *
 */
class LocalTupleEntryCollector extends TupleEntrySchemeCollector
  {
  private final Closeable writer;

  public LocalTupleEntryCollector( LocalFlowProcess flowProcess, Scheme scheme, Closeable writer )
    {
    super( flowProcess, scheme, writer );

    if( writer == null )
      throw new IllegalArgumentException( "writer may not be null" );

    this.writer = writer;
    }

  @Override
  public void close()
    {
    super.close();

    try
      {
      writer.close();
      }
    catch( IOException exception )
      {
      // ignore
      }
    }
  }
