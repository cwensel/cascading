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

package cascading.tap.hadoop.util;

import java.io.Closeable;
import java.io.IOException;

import cascading.flow.FlowProcess;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class MeasuredOutputCollector implements OutputCollector, Closeable
  {
  private final FlowProcess flowProcess;
  private final Enum counter;

  private OutputCollector outputCollector;

  public MeasuredOutputCollector( FlowProcess flowProcess, Enum counter )
    {
    this.flowProcess = flowProcess;
    this.counter = counter;
    }

  public MeasuredOutputCollector( FlowProcess flowProcess, Enum counter, OutputCollector outputCollector )
    {
    this.flowProcess = flowProcess;
    this.counter = counter;
    this.outputCollector = outputCollector;
    }

  public OutputCollector getOutputCollector()
    {
    return outputCollector;
    }

  public void setOutputCollector( OutputCollector outputCollector )
    {
    this.outputCollector = outputCollector;
    }

  @Override
  public void collect( Object key, Object value ) throws IOException
    {
    long start = System.currentTimeMillis();

    try
      {
      outputCollector.collect( key, value );
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public void close() throws IOException
    {
    if( outputCollector instanceof Closeable )
      ( (Closeable) outputCollector ).close();
    }
  }
