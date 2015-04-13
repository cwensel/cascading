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

package cascading.stats.tez.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import cascading.CascadingException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.common.ATSConstants;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGClientTimelineImpl;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;
import org.apache.tez.dag.api.client.VertexStatus;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.tez.common.ATSConstants.*;
import static org.apache.tez.dag.history.logging.EntityTypes.TEZ_TASK_ID;

/**
 *
 */
public class TezTimelineClient extends DAGClientTimelineImpl implements TimelineClient
  {
  private static final Logger LOG = LoggerFactory.getLogger( TezTimelineClient.class );

  private static final String FILTER_BY_FIELDS = "primaryfilters,otherinfo";

  private final String dagId;
  private final DAGClient dagClient;

  public TezTimelineClient( ApplicationId appId, String dagId, TezConfiguration conf, FrameworkClient frameworkClient, DAGClient dagClient ) throws TezException
    {
    super( appId, dagId, conf, frameworkClient );
    this.dagId = dagId;
    this.dagClient = dagClient;
    }

  public DAGClient getDAGClient()
    {
    return dagClient;
    }

  @Override
  public DAGStatus getDAGStatus( @Nullable Set<StatusGetOpts> statusOptions ) throws IOException, TezException
    {
    return dagClient.getDAGStatus( statusOptions );
    }

  @Override
  public VertexStatus getVertexStatus( String vertexName, Set<StatusGetOpts> statusOptions ) throws IOException, TezException
    {
    return dagClient.getVertexStatus( vertexName, statusOptions );
    }

  @Override
  public String getVertexID( String vertexName ) throws IOException, TezException
    {
    String format = "%s/%s?primaryFilter=%s:%s&secondaryFilter=vertexName:%s&fields=%s";
    String url = String.format( format, baseUri, TEZ_VERTEX_ID, TEZ_DAG_ID, dagId, vertexName, "primaryfilters" );

    JSONObject jsonRoot = getJsonRootEntity( url );
    JSONArray entitiesNode = jsonRoot.optJSONArray( ENTITIES );

    if( entitiesNode == null || entitiesNode.length() != 1 )
      throw new CascadingException( "failed to get vertex status from timeline server" );

    try
      {
      return getRemoveJsonObject( entitiesNode, 0, true ).getString( ENTITY );
      }
    catch( JSONException exception )
      {
      throw new CascadingException( "unable to get vertex node", exception );
      }
    }

  @Override
  public Iterator<TaskStatus> getVertexChildren( String vertexID, int limit, String startTaskID ) throws IOException, TezException
    {
    if( vertexID == null )
      throw new IllegalArgumentException( "vertexID is required" );

    String format = "%s/%s?primaryFilter=%s:%s&fields=%s&limit=%s";
    String url = String.format( format, baseUri, TEZ_TASK_ID, TEZ_VERTEX_ID, vertexID, FILTER_BY_FIELDS, limit );

    if( startTaskID != null )
      url = String.format( "%s&fromId=%s", url, startTaskID );

    JSONObject jsonRoot = getJsonRootEntity( url );
    final JSONArray entitiesNode = jsonRoot.optJSONArray( ATSConstants.ENTITIES );

    if( entitiesNode == null )
      throw new CascadingException( "failed to get vertex task statuses from timeline server" );

    LOG.info( "retrieved {} tasks", entitiesNode.length() );

    return new Iterator<TaskStatus>()
    {
    @Override
    public boolean hasNext()
      {
      return entitiesNode.length() != 0;
      }

    @Override
    public TaskStatus next()
      {
      // remove for gc as we accumulate a replacement SliceStats instance
      return parseTaskStatus( getRemoveJsonObject( entitiesNode, 0, true ) );
      }

    @Override
    public void remove()
      {

      }
    };
    }

  @Override
  public TaskStatus getVertexChild( String taskID ) throws TezException
    {
    String format = "%s/%s/%s?fields=%s";
    String url = String.format( format, baseUri, TEZ_TASK_ID, taskID, FILTER_BY_FIELDS );

    JSONObject jsonRoot = getJsonRootEntity( url );

    if( jsonRoot == null )
      throw new CascadingException( "failed to get vertex task status from timeline server, for id: " + taskID );

    return parseTaskStatus( jsonRoot );
    }

  private TaskStatus parseTaskStatus( JSONObject jsonRoot )
    {
    try
      {
      String taskID = jsonRoot.optString( ATSConstants.ENTITY );
      JSONObject otherInfoNode = jsonRoot.getJSONObject( ATSConstants.OTHER_INFO );
      String status = otherInfoNode.optString( ATSConstants.STATUS );
      String diagnostics = otherInfoNode.optString( ATSConstants.DIAGNOSTICS );

      if( status.equals( "" ) )
        return new TaskStatus( taskID );

      JSONObject countersNode = otherInfoNode.optJSONObject( ATSConstants.COUNTERS );
      Map<String, Map<String, Long>> counters = parseDagCounters( countersNode );

      return new TaskStatus( taskID, status, counters, diagnostics );
      }
    catch( JSONException exception )
      {
      throw new CascadingException( exception );
      }
    }

  private Map<String, Map<String, Long>> parseDagCounters( JSONObject countersNode ) throws JSONException
    {
    if( countersNode == null )
      return null;

    JSONArray counterGroupNodes = countersNode.optJSONArray( ATSConstants.COUNTER_GROUPS );

    if( counterGroupNodes == null )
      return null;

    Map<String, Map<String, Long>> counters = new HashMap<>();
    int numCounterGroups = counterGroupNodes.length();

    for( int i = 0; i < numCounterGroups; i++ )
      parseCounterGroup( counters, counterGroupNodes.optJSONObject( i ) );

    return counters;
    }

  private void parseCounterGroup( Map<String, Map<String, Long>> counters, JSONObject counterGroupNode ) throws JSONException
    {
    if( counterGroupNode == null )
      return;

    final String groupName = counterGroupNode.optString( ATSConstants.COUNTER_GROUP_NAME );
//    final String groupDisplayName = counterGroupNode.optString( ATSConstants.COUNTER_GROUP_DISPLAY_NAME );
    final JSONArray counterNodes = counterGroupNode.optJSONArray( ATSConstants.COUNTERS );
    final int numCounters = counterNodes.length();

    Map<String, Long> values = new HashMap<>();

    counters.put( groupName, values );

    for( int i = 0; i < numCounters; i++ )
      {
      JSONObject counterNode = counterNodes.getJSONObject( i );
      String counterName = counterNode.getString( ATSConstants.COUNTER_NAME );
//      String counterDisplayName = counterNode.getString( ATSConstants.COUNTER_DISPLAY_NAME );
      long counterValue = counterNode.getLong( ATSConstants.COUNTER_VALUE );

      values.put( counterName, counterValue );
      }
    }

  protected JSONObject getRemoveJsonObject( JSONArray entitiesNode, int index, boolean doRemove )
    {
    try
      {
      JSONObject jsonObject = entitiesNode.getJSONObject( index );

      if( doRemove )
        entitiesNode.remove( jsonObject );

      return jsonObject;
      }
    catch( JSONException exception )
      {
      throw new CascadingException( exception );
      }
    }
  }
