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

package cascading.flow.tez.stream.element;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

import cascading.CascadingException;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 *
 */
class OldOutputCollector implements OutputCollector, Flushable, Closeable
  {
  private final LogicalOutput logicalOutput;
  private final KeyValueWriter output;

  OldOutputCollector( LogicalOutput logicalOutput )
    {
    this.logicalOutput = logicalOutput;

    try
      {
      this.output = (KeyValueWriter) logicalOutput.getWriter();
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to get writer", exception );
      }
    }

  public void collect( Object key, Object value ) throws IOException
    {
    output.write( key, value );
    }

  @Override
  public void flush() throws IOException
    {
    if( logicalOutput instanceof MROutput )
      ( (MROutput) logicalOutput ).flush();
    }

  @Override
  public void close() throws IOException
    {
    if( logicalOutput instanceof MROutput )
      ( (MROutput) logicalOutput ).close();
    }
  }
