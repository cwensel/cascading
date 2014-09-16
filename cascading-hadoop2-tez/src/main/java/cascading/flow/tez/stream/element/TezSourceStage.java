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

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.flow.stream.element.SourceStage;
import cascading.flow.tez.Hadoop2TezFlowProcess;
import cascading.flow.tez.util.TezUtil;
import cascading.tap.Tap;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.runtime.api.LogicalInput;

/**
 *
 */
public class TezSourceStage extends SourceStage
  {
  private final MRInputLegacy logicalInput;

  public TezSourceStage( FlowProcess flowProcess, Tap source, LogicalInput logicalInput )
    {
    super( flowProcess, source );

    if( logicalInput == null )
      throw new IllegalArgumentException( "input must not be null" );

    this.logicalInput = (MRInputLegacy) logicalInput;
    }

  @Override
  public void prepare()
    {
    try
      {
      logicalInput.init();
      }
    catch( IOException exception )
      {
      throw new CascadingException( "unable to init input", exception );
      }

    logicalInput.start();

    Hadoop2TezFlowProcess tezFlowProcess;

    if( flowProcess instanceof FlowProcessWrapper )
      tezFlowProcess = (Hadoop2TezFlowProcess) ( (FlowProcessWrapper) flowProcess ).getDelegate();
    else
      tezFlowProcess = (Hadoop2TezFlowProcess) flowProcess;

    TezConfiguration configuration = tezFlowProcess.getConfiguration();

    // set the cascading.source.path property for the current split
    // if a TezGroupedSplit, currently won't set
    TezUtil.setSourcePathForSplit( logicalInput, configuration );
    }

  @Override
  public void run( Object input ) throws Throwable
    {
    RecordReader oldRecordReader = logicalInput.getOldRecordReader();

    super.run( oldRecordReader );
    }
  }
