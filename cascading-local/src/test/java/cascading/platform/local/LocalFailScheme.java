/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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
  public boolean source( FlowProcess<? extends Properties> flowProcess, SourceCall<LineNumberReader, InputStream> sourceCall ) throws IOException
    {
    if( !sourceFired )
      {
      sourceFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    return super.source( flowProcess, sourceCall );
    }

  @Override
  public void sink( FlowProcess<? extends Properties> flowProcess, SinkCall<PrintWriter, OutputStream> sinkCall ) throws IOException
    {
    if( !sinkFired )
      {
      sinkFired = true;
      throw new TapException( "fail", new Tuple( "bad data" ) );
      }

    super.sink( flowProcess, sinkCall );
    }
  }
