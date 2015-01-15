/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.stats.hadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import cascading.CascadingException;
import cascading.flow.Flow;
import cascading.management.state.ClientState;
import cascading.stats.FlowStats;
import cascading.stats.FlowStepStats;
import riffle.process.scheduler.ProcessException;
import riffle.process.scheduler.ProcessWrapper;

/**
 * ProcessFlowStats is a sub-class of FlowStats which can fetch counters from a ProcessWrapper and hook them into the
 * stats mechanism of Cascading.
 */
public class ProcessFlowStats extends FlowStats
  {
  /** The ProcessWrapper having the actual counters. */
  private ProcessWrapper processWrapper;

  /**
   * Constructs a new ProcessFlowStats instance.
   *
   * @param flow           a Flow instance.
   * @param clientState    The current client state.
   * @param processWrapper a ProcessWrapper instance.
   */
  public ProcessFlowStats( Flow flow, ClientState clientState, ProcessWrapper processWrapper )
    {
    super( flow, clientState );
    this.processWrapper = processWrapper;
    }

  @Override
  public List<FlowStepStats> getFlowStepStats()
    {
    return getChildrenInternal();
    }

  @Override
  public Collection getChildren()
    {
    return getChildrenInternal();
    }

  /**
   * Internal method to retrieve the child stats objects from the ProcessWrapper.
   */
  private List<FlowStepStats> getChildrenInternal()
    {
    try
      {
      if( !processWrapper.hasChildren() )
        {
        if( processWrapper.hasCounters() )
          return Arrays.<FlowStepStats>asList(
            new ProcessStepStats( clientState, processWrapper.getCounters(), new ProcessFlowStep( processWrapper, 1 ) ) );
        else
          return Collections.emptyList();
        }

      List<FlowStepStats> childStepStats = new ArrayList<FlowStepStats>();
      int counter = 0;
      for( Object process : processWrapper.getChildren() )
        {
        ProcessWrapper childWrapper = new ProcessWrapper( process );
        if( childWrapper.hasCounters() )
          {
          ProcessStepStats processStepStats = new ProcessStepStats( clientState, childWrapper.getCounters(),
            new ProcessFlowStep( processWrapper, counter ) );

          counter++;

          childStepStats.add( processStepStats );
          }
        }
      return childStepStats;
      }
    catch( ProcessException exception )
      {
      throw new CascadingException( exception );
      }
    }

  @Override
  public int getStepsCount()
    {
    try
      {
      if( !processWrapper.hasChildren() )
        return 1; // there is always a step, even if it is opaque to us

      return processWrapper.getChildren().size();
      }
    catch( ProcessException exception )
      {
      throw new CascadingException( exception );
      }
    }
  }
