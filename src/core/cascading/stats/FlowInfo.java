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

package cascading.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.Scope;
import cascading.flow.planner.ElementGraph;
import cascading.flow.planner.FlowStep;
import cascading.tap.Tap;

/**
 *
 */
public class FlowInfo
  {
  private Flow flow;
  private ElementGraph pipeGraph;
  private String flowID;
  private String appID;
  private String appName;
  private String appVersion;
  private TapsInfo sourceInfo;
  private TapsInfo sinkInfo;
  private TapsInfo trapInfo;
  private List<FlowStep> steps;

  public FlowInfo( Flow flow )
    {
    this.flow = flow;
    this.flowID = flow.getID();
    this.appID = flow.getAppID();
    this.appName = flow.getAppName();
    this.appVersion = flow.getAppVersion();
    this.sourceInfo = new TapsInfo( flow.getSourcesCollection() );
    this.sinkInfo = new TapsInfo( flow.getSinksCollection() );
    this.trapInfo = new TapsInfo( flow.getTrapsCollection() );

    this.steps = flow.getSteps();

    this.pipeGraph = flow.getPipeGraph();
    }

  public String getFlowID()
    {
    return flowID;
    }

  public String getAppID()
    {
    return appID;
    }

  public String getAppName()
    {
    return appName;
    }

  public String getAppVersion()
    {
    return appVersion;
    }

  public TapsInfo getSourceInfo()
    {
    return sourceInfo;
    }

  public TapsInfo getSinkInfo()
    {
    return sinkInfo;
    }

  public TapsInfo getTrapInfo()
    {
    return trapInfo;
    }

  public List<String> getStepIDs()
    {
    List<String> ids = new ArrayList<String>( steps.size() );

    for( FlowStep step : steps )
      ids.add( step.getID() );

    return ids;
    }

  public ElementGraph getPipeGraph()
    {
    return pipeGraph;
    }

  public Map<Scope, Integer> getEdges()
    {
    return makeMap( pipeGraph.edgeSet() );
    }

  public Map<FlowElement, Integer> getVertices()
    {
    return makeMap( pipeGraph.vertexSet() );
    }

  public FlowElement getEdgeSource( Scope scope )
    {
    return pipeGraph.getEdgeSource( scope );
    }

  public FlowElement getEdgeTarget( Scope scope )
    {
    return pipeGraph.getEdgeTarget( scope );
    }

  public String getType( FlowElement flowElement )
    {
    if( isExtent( flowElement ) )
      return ( (ElementGraph.Extent) flowElement ).getName().toUpperCase();

    if( flowElement instanceof Tap )
      return "TAP";

    return flowElement.getClass().getSimpleName().toUpperCase();
    }

  public boolean isExtent( FlowElement flowElement )
    {
    return flowElement instanceof ElementGraph.Extent;
    }

  private Map makeMap( Collection collection )
    {
    Map map = new LinkedHashMap();
    int count = 0;

    for( Object o : collection )
      map.put( o, count++ );

    return map;
    }

  }
