/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.tuple.hadoop.util;

import cascading.tuple.Tuple;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/** Class GroupingPartitioner is an implementation of {@link org.apache.hadoop.mapred.Partitioner}. */
public class GroupingPartitioner extends HasherPartitioner implements Partitioner<Tuple, Tuple>
  {
  public int getPartition( Tuple key, Tuple value, int numReduceTasks )
    {
    return ( hashCode( key ) & Integer.MAX_VALUE ) % numReduceTasks;
    }

  @Override
  public void configure( JobConf job )
    {
    setConf( job );
    }
  }
