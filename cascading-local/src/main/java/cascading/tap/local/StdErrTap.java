/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntrySchemeCollector;

/** Class StdErrTap provides a local mode tap for writing data to the {@code stderr} stream. */
public class StdErrTap extends SinkTap<Properties, OutputStream>
  {
  public StdErrTap( Scheme<Properties, ?, OutputStream, ?, ?> scheme )
    {
    super( scheme, SinkMode.UPDATE );
    }

  @Override
  public String getIdentifier()
    {
    return "stdErr";
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, OutputStream output ) throws IOException
    {
    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, getScheme(), System.err );
    }

  @Override
  public boolean createResource( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    return false;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return 0;
    }
  }
