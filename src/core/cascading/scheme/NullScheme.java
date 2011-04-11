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

package cascading.scheme;

import java.io.IOException;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 *
 */
public class NullScheme<Process extends FlowProcess, Config, Input, Output, Context> extends Scheme<Process, Config, Input, Output, Context>
  {
  public NullScheme()
    {
    }

  public NullScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  public void sourceConfInit( Process flowProcess, Tap tap, Config conf ) throws IOException
    {
    }

  public void sinkConfInit( Process flowProcess, Tap tap, Config conf ) throws IOException
    {
    }

  public boolean source( Process flowProcess, SourceCall<Context, Input> sourceCall ) throws IOException
    {
    throw new UnsupportedOperationException( "sourcing is not supported in the scheme" );
    }

  public void sink( Process flowProcess, SinkCall<Context, Output> sinkCall ) throws IOException
    {
    throw new UnsupportedOperationException( "sinking is not supported in the scheme" );
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName();
    }
  }
