/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.stream.HadoopGroupGate;
import cascading.flow.stream.element.InputSource;
import cascading.flow.stream.graph.IORole;
import cascading.pipe.Splice;
import cascading.util.SortedListMultiMap;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.library.output.OrderedPartitionedKVOutput;

/**
 *
 */
public abstract class TezGroupGate extends HadoopGroupGate implements InputSource
  {
  protected OrderedPartitionedKVOutput logicalOutput;
  protected SortedListMultiMap<Integer, LogicalInput> logicalInputs;

  public TezGroupGate( FlowProcess flowProcess, Splice splice, IORole role, LogicalOutput logicalOutput )
    {
    super( flowProcess, splice, role );

    if( logicalOutput == null )
      throw new IllegalArgumentException( "output must not be null" );

    this.logicalOutput = (OrderedPartitionedKVOutput) logicalOutput;
    }

  public TezGroupGate( FlowProcess flowProcess, Splice splice, IORole role, SortedListMultiMap<Integer, LogicalInput> logicalInputs )
    {
    super( flowProcess, splice, role );

    if( logicalInputs == null || logicalInputs.getKeys().size() == 0 )
      throw new IllegalArgumentException( "inputs must not be null or empty" );

    this.logicalInputs = logicalInputs;
    }

  @Override
  public void initialize()
    {
    super.initialize();

    if( role == IORole.sink )
      return;

    initComparators();
    }

  @Override
  public void prepare()
    {
    try
      {
      if( logicalInputs != null )
        {
        for( LogicalInput logicalInput : logicalInputs.getValues() )
          logicalInput.start();
        }

      if( logicalOutput != null )
        logicalOutput.start();
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to start", exception );
      }

    super.prepare();
    }

  @Override
  public void run( Object input ) throws Throwable
    {
    Throwable throwable = reduce();

    if( throwable != null )
      throw throwable;
    }

  protected abstract Throwable reduce() throws Exception;

  @Override
  protected OutputCollector createOutputCollector()
    {
    return new OldOutputCollector( logicalOutput );
    }
  }
