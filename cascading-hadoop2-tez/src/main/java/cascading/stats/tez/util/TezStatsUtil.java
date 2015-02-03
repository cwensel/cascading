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

package cascading.stats.tez.util;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

import cascading.CascadingException;
import cascading.stats.CascadingStats;
import cascading.util.Util;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TezStatsUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezStatsUtil.class );

  public static final Set<StatusGetOpts> STATUS_GET_COUNTERS = EnumSet.of( StatusGetOpts.GET_COUNTERS );

  public static DAGStatus getDagStatusWithCounters( DAGClient dagClient )
    {
    if( dagClient == null )
      return null;

    try
      {
      return dagClient.getDAGStatus( STATUS_GET_COUNTERS );
      }
    catch( IOException | TezException exception )
      {
      throw new CascadingException( "unable to get counters from dag client", exception );
      }
    }

  public static TimelineClient createTimelineClient( DAGClient dagClient, CascadingStats stats )
    {
    if( dagClient == null )
      return null;

    ApplicationId appId = Util.returnInstanceFieldIfExistsSafe( dagClient, "appId" );
    String dagId = Util.returnInstanceFieldIfExistsSafe( dagClient, "dagId" );
    TezConfiguration conf = Util.returnInstanceFieldIfExistsSafe( dagClient, "conf" );
    FrameworkClient frameworkClient = Util.returnInstanceFieldIfExistsSafe( dagClient, "frameworkClient" );

    try
      {
      return new TimelineClient( appId, dagId, conf, frameworkClient, dagClient );
      }
    catch( TezException exception )
      {
      LOG.warn( "unable to construct timeline server client" );
      }

    return null;
    }
  }
