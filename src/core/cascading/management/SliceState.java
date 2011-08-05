/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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
    initialize( currentProcess.getCurrentSession().getCascadingServices(), ClientType.slice, String.valueOf( currentProcess.getCurrentTaskNum() ) );
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
