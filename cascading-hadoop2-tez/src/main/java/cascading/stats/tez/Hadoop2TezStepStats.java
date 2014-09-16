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

package cascading.stats.tez;

import java.util.Collection;

import cascading.flow.FlowStep;
import cascading.management.state.ClientState;
import cascading.stats.FlowStepStats;

/**
 *
 */
public class Hadoop2TezStepStats extends FlowStepStats
  {
  /**
   * Constructor CascadingStats creates a new CascadingStats instance.
   *
   * @param flowStep
   * @param clientState
   */
  protected Hadoop2TezStepStats( FlowStep flowStep, ClientState clientState )
    {
    super( flowStep, clientState );
    }

  @Override
  public void recordChildStats()
    {

    }

  @Override
  public Collection<String> getCounterGroups()
    {
    return null;
    }

  @Override
  public Collection<String> getCounterGroupsMatching( String regex )
    {
    return null;
    }

  @Override
  public Collection<String> getCountersFor( String group )
    {
    return null;
    }

  @Override
  public long getCounterValue( Enum counter )
    {
    return 0;
    }

  @Override
  public long getCounterValue( String group, String counter )
    {
    return 0;
    }

  @Override
  public void captureDetail()
    {

    }

  @Override
  public Collection getChildren()
    {
    return null;
    }
  }
