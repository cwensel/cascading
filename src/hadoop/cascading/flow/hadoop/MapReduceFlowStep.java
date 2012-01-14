/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.hadoop;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.JobConf;

/** Class MapReduceFlowStep wraps a {@link JobConf} and allows it to be executed as a {@link cascading.flow.Flow}. */
public class MapReduceFlowStep extends HadoopFlowStep
  {
  /** Field jobConf */
  private final JobConf jobConf;

  MapReduceFlowStep( String name, JobConf jobConf, Tap sink )
    {
    super( name, 1 );
    this.jobConf = jobConf;
    addSink( "default", sink );
    }

  @Override
  public JobConf getInitializedConfig( FlowProcess<JobConf> flowProcess, JobConf parentConfig )
    {
    // allow to delete
    getSink().sinkConfInit( flowProcess, new JobConf() );

    return jobConf;
    }
  }
