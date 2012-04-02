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

package cascading.tuple.hadoop;

import java.util.Collection;

import cascading.flow.FlowProcess;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollectionFactory;
import org.apache.hadoop.io.compress.CompressionCodec;

import static cascading.tuple.SpillableTupleList.defaultThreshold;
import static cascading.tuple.SpillableTupleList.getThreshold;
import static cascading.tuple.hadoop.HadoopSpillableTupleList.defaultCodecs;
import static cascading.tuple.hadoop.HadoopSpillableTupleList.getCodec;

/**
 *
 */
public class HadoopTupleCollectionFactory implements TupleCollectionFactory
  {
  private int spillThreshold;
  private CompressionCodec codec;
  private TupleSerialization tupleSerialization;

  @Override
  public void initialize( FlowProcess flowProcess )
    {
    this.spillThreshold = getThreshold( flowProcess, defaultThreshold );
    this.codec = getCodec( flowProcess, defaultCodecs );

    this.tupleSerialization = new TupleSerialization( flowProcess );
    }

  @Override
  public Collection<Tuple> create( FlowProcess flowProcess )
    {
    return new HadoopSpillableTupleList( spillThreshold, tupleSerialization, codec );
    }
  }
