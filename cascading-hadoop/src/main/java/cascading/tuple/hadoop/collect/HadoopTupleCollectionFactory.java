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

package cascading.tuple.hadoop.collect;

import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.collect.TupleCollectionFactory;
import cascading.tuple.hadoop.TupleSerialization;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.JobConf;

import static cascading.tuple.collect.SpillableProps.defaultListThreshold;
import static cascading.tuple.collect.SpillableTupleList.getThreshold;

/**
 *
 */
public class HadoopTupleCollectionFactory implements TupleCollectionFactory<JobConf>
  {
  private int spillThreshold;
  private CompressionCodec codec;
  private TupleSerialization tupleSerialization;

  @Override
  public void initialize( FlowProcess<JobConf> flowProcess )
    {
    this.spillThreshold = getThreshold( flowProcess, defaultListThreshold );
    this.codec = HadoopSpillableTupleList.getCodec( flowProcess, HadoopSpillableTupleList.defaultCodecs );

    this.tupleSerialization = new TupleSerialization( flowProcess );
    }

  @Override
  public Collection<Tuple> create( FlowProcess<JobConf> flowProcess )
    {
    return new HadoopSpillableTupleList( spillThreshold, tupleSerialization, codec );
    }
  }
