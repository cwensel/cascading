/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow;

import java.io.IOException;

import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

/** Class MapReduceFlowStep wraps a {@link JobConf} and allows it to be executed as a {@link Flow}. */
public class MapReduceFlowStep extends FlowStep
  {
  /** Field jobConf */
  private final JobConf jobConf;

  MapReduceFlowStep( String name, JobConf jobConf, Tap sink )
    {
    super( name, 1 );
    this.jobConf = jobConf;
    this.sink = sink;
    }

  @Override
  protected JobConf getJobConf( JobConf parentConf ) throws IOException
    {
    // allow to delete
    sink.sinkInit( new JobConf() );

    return jobConf;
    }
  }
