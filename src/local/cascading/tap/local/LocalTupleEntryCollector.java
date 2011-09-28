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
