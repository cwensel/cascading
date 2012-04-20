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

package cascading.tap.local;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.local.LocalScheme;
import cascading.tuple.TupleEntrySchemeCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class LocalTupleEntryCollector extends TupleEntrySchemeCollector<Closeable>
  {
  private static final Logger LOG = LoggerFactory.getLogger( LocalTupleEntryCollector.class );

  private final Closeable writer;

  public LocalTupleEntryCollector( FlowProcess<Properties> flowProcess, LocalScheme scheme, FileOutputStream output, String identifier )
    {
    this( flowProcess, scheme, (Writer) scheme.createOutput( output ), identifier );
    }

  public LocalTupleEntryCollector( FlowProcess<Properties> flowProcess, LocalScheme scheme, Writer writer, String identifier )
    {
    super( flowProcess, scheme, writer, identifier );

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
      if( writer instanceof Flushable )
        ( (Flushable) writer ).flush();

      writer.close();
      }
    catch( IOException exception )
      {
      LOG.error( "unable to close writer, not rethrowing exception", exception );
      }
    }
  }
