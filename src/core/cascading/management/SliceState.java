/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.management;

import cascading.flow.FlowProcess;

/**
 *
 */
public abstract class SliceState extends BaseState
  {
  String sessionID;
  String processID;
  String sliceID;
  protected String stage;

  protected SliceState( FlowProcess currentProcess )
    {
    initialize( currentProcess.getCurrentSession().getCascadingServices(), String.valueOf( currentProcess.getCurrentTaskNum() ) );
    this.sessionID = currentProcess.getCurrentSession().getID();
    this.processID = currentProcess.getID();
    this.sliceID = String.valueOf( currentProcess.getCurrentTaskNum() );
    }

  @Override
  String[] getContext( String group, String metric )
    {
    return asArray( stage, group, metric, sessionID, processID, sliceID );
    }

  // LIFECYCLE METRIC METHODS

  public void start()
    {
    metricsService.startService();

    setMetric( "state", "start", System.currentTimeMillis() );
    }

  public void stop()
    {
    setMetric( "state", "stop", System.currentTimeMillis() );

    metricsService.stopService();
    }
  }
