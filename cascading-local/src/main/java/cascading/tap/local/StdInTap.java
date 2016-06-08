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

package cascading.tap.local;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SourceTap;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeIterator;

/** Class StdInTap provides a local mode tap for reading data from the {@code stdin} stream. */
public class StdInTap extends SourceTap<Properties, InputStream>
  {
  public StdInTap( Scheme<Properties, InputStream, ?, ?, ?> scheme )
    {
    super( scheme );
    }

  @Override
  public String getIdentifier()
    {
    return "stdIn";
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, InputStream inputStream ) throws IOException
    {
    return new TupleEntrySchemeIterator<Properties, InputStream>( flowProcess, getScheme(), System.in );
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return System.currentTimeMillis();
    }
  }
