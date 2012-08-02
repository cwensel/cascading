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

package cascading.platform.local;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.TapException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 *
 */
public class LocalFailScheme extends cascading.scheme.local.TextLine
  {
  boolean sourceFired = false;
  boolean sinkFired = false;

  public LocalFailScheme()
    {
    }

  public LocalFailScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  @Override
  public boolean source( FlowProcess<Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    if( !sourceFired )
      {
      sourceFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    return super.source( flowProcess, sourceCall );
    }

  @Override
  public void sink( FlowProcess<Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
    {
    if( !sinkFired )
      {
      sinkFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    super.sink( flowProcess, sinkCall );
    }
  }
