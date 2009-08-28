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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

/**
 *
 */
public class HfsContext extends TaskInputOutputContext
  {
  public HfsContext( Configuration conf, TaskAttemptID taskid, RecordWriter output, OutputCommitter committer, StatusReporter reporter )
    {
    super( conf, taskid, output, committer, reporter );
    }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
    {
    return false;
    }

  @Override
  public Object getCurrentKey() throws IOException, InterruptedException
    {
    return null;
    }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException
    {
    return null;
    }
  }
