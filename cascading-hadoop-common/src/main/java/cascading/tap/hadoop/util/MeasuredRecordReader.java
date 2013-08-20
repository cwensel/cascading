/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;

import cascading.flow.FlowProcess;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 */
public class MeasuredRecordReader implements RecordReader
  {
  private final FlowProcess flowProcess;
  private final Enum counter;

  private RecordReader recordReader;

  public MeasuredRecordReader( FlowProcess flowProcess, Enum counter )
    {
    this.flowProcess = flowProcess;
    this.counter = counter;
    }

  public RecordReader getRecordReader()
    {
    return recordReader;
    }

  public void setRecordReader( RecordReader recordReader )
    {
    this.recordReader = recordReader;
    }

  @Override
  public boolean next( Object key, Object value ) throws IOException
    {
    long start = System.currentTimeMillis();

    try
      {
      return recordReader.next( key, value );
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public Object createKey()
    {
    long start = System.currentTimeMillis();

    try
      {
      return recordReader.createKey();
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public Object createValue()
    {
    long start = System.currentTimeMillis();

    try
      {
      return recordReader.createValue();
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public long getPos() throws IOException
    {
    return recordReader.getPos();
    }

  @Override
  public void close() throws IOException
    {
    long start = System.currentTimeMillis();

    try
      {
      recordReader.close();
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }

  @Override
  public float getProgress() throws IOException
    {
    long start = System.currentTimeMillis();

    try
      {
      return recordReader.getProgress();
      }
    finally
      {
      flowProcess.increment( counter, System.currentTimeMillis() - start );
      }
    }
  }
