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

package cascading.tap.hadoop;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 */
public class MultiInputRecordReader extends RecordReader
  {
  RecordReader recordReader;

  public MultiInputRecordReader( RecordReader recordReader )
    {
    this.recordReader = recordReader;
    }

  @Override
  public void initialize( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException
    {
    recordReader.initialize( ( (MultiInputSplit) split ).inputSplit, context );
    }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
    {
    return recordReader.nextKeyValue();
    }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException
    {
    return recordReader.getCurrentValue();
    }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException
    {
    return recordReader.getCurrentValue();
    }

  @Override
  public float getProgress() throws IOException, InterruptedException
    {
    return recordReader.getProgress();
    }

  @Override
  public void close() throws IOException
    {
    recordReader.close();
    }
  }
