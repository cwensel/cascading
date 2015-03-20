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

package cascading.flow.stream.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowElement;
import cascading.flow.FlowNode;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.AnnotatedGraph;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.graph.Extent;
import cascading.flow.stream.annotations.BlockingMode;
import cascading.flow.stream.duct.Duct;
import cascading.flow.stream.duct.Gate;
import cascading.flow.stream.element.AggregatorEveryStage;
import cascading.flow.stream.element.BufferEveryWindow;
import cascading.flow.stream.element.ElementDuct;
import cascading.flow.stream.element.ElementFlowProcess;
import cascading.flow.stream.element.FilterEachStage;
import cascading.flow.stream.element.FunctionEachStage;
import cascading.flow.stream.element.GroupAssertionEveryStage;
import cascading.flow.stream.element.GroupingSpliceGate;
import cascading.flow.stream.element.MemoryCoGroupGate;
import cascading.flow.stream.element.MemoryHashJoinGate;
import cascading.flow.stream.element.MergeStage;
import cascading.flow.stream.element.SinkStage;
import cascading.flow.stream.element.SourceStage;
import cascading.flow.stream.element.TrapHandler;
import cascading.flow.stream.element.ValueAssertionEachStage;
import cascading.pipe.Boundary;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import cascading.util.Util;
import org.jgrapht.Graphs;

/**
 *
 */
public abstract class NodeStreamGraph extends StreamGraph
  {
  protected FlowProcess flowProcess;
  protected final FlowNode node;
  protected FlowElement streamedSource;
  protected final ElementGraph elementGraph;

  public NodeStreamGraph( FlowProcess flowProcess, FlowNode node )
    {
    this.flowProcess = flowProcess;
    this.node = node;
    this.elementGraph = node.getElementGraph();
    }

  public NodeStreamGraph( FlowProcess flowProcess, FlowNode node, FlowElement streamedSource )
    {
    this.flowProcess = flowProcess;
    this.node = node;
    this.elementGraph = streamedSource == null ? node.getElementGraph() : node.getPipelineGraphFor( streamedSource );
    this.streamedSource = streamedSource;
    }

  protected Object getProperty( String name )
    {
    return flowProcess.getProperty( name );
    }

  protected void handleDuct( FlowElement lhsElement, Duct lhsDuct )
    {
    List<FlowElement> successors = Graphs.successorListOf( elementGraph, lhsElement );

    if( successors.contains( Extent.tail ) )
      addTail( lhsDuct );
    else
      handleSuccessors( lhsDuct, successors );
    }

  private void handleSuccessors( Duct lhsDuct, List<FlowElement> successors )
    {
    for( FlowElement rhsElement : successors )
      {
      if( rhsElement instanceof Extent )
        continue;

      boolean isSink = Graphs.successorListOf( elementGraph, rhsElement ).contains( Extent.tail );
      boolean isSource = Graphs.predecessorListOf( elementGraph, rhsElement ).contains( Extent.head );

      IORole role = IORole.pass;

      if( isSource && !isSink )
        role = IORole.source;
      else if( !isSource && isSink )
        role = IORole.sink;
      else if( isSource && isSink )
        role = IORole.both;

      Duct newRhsDuct = createDuctFor( rhsElement, role );
      Duct rhsDuct = findExisting( newRhsDuct );

      int ordinal = findEdgeOrdinal( lhsDuct, rhsDuct );

      addPath( lhsDuct, ordinal, rhsDuct );

      if( rhsDuct != newRhsDuct ) // don't keep going if we have already seen rhs
        continue;

      handleDuct( rhsElement, rhsDuct );
      }
    }

  private int findEdgeOrdinal( Duct lhsDuct, Duct rhsDuct )
    {
    if( !( rhsDuct instanceof GroupingSpliceGate ) )
      return 0;

    FlowElement lhsElement = ( (ElementDuct) lhsDuct ).getFlowElement();
    FlowElement rhsElement = ( (ElementDuct) rhsDuct ).getFlowElement();

    Set<Scope> allEdges = elementGraph.getAllEdges( lhsElement, rhsElement );

    if( allEdges.size() == 1 )
      return Util.getFirst( allEdges ).getOrdinal();

    throw new IllegalStateException( "could not find ordinal, too many edges between elements" );
    }

  private Duct createDuctFor( FlowElement element, IORole role )
    {
    Duct rhsDuct;

    if( element instanceof Each )
      {
      Each eachElement = (Each) element;

      if( eachElement.isFunction() )
        rhsDuct = new FunctionEachStage( flowProcess, eachElement );
      else if( eachElement.isFilter() )
        rhsDuct = new FilterEachStage( flowProcess, eachElement );
      else if( eachElement.isValueAssertion() )
        rhsDuct = new ValueAssertionEachStage( flowProcess, eachElement );
      else
        throw new IllegalStateException( "unknown operation: " + eachElement.getOperation().getClass().getCanonicalName() );
      }
    else if( element instanceof Every )
      {
      Every everyElement = (Every) element;

      if( everyElement.isBuffer() )
        rhsDuct = new BufferEveryWindow( flowProcess, everyElement );
      else if( everyElement.isAggregator() )
        rhsDuct = new AggregatorEveryStage( flowProcess, everyElement );
      else if( everyElement.isGroupAssertion() )
        rhsDuct = new GroupAssertionEveryStage( flowProcess, everyElement );
      else
        throw new IllegalStateException( "unknown operation: " + everyElement.getOperation().getClass().getCanonicalName() );
      }
    else if( element instanceof Boundary )
      {
      rhsDuct = createBoundaryStage( (Boundary) element, role );
      }
    else if( element instanceof Splice )
      {
      Splice spliceElement = (Splice) element;

      if( spliceElement.isGroupBy() )
        rhsDuct = createGroupByGate( (GroupBy) spliceElement, role );
      else if( spliceElement.isCoGroup() )
        rhsDuct = createCoGroupGate( (CoGroup) spliceElement, role );
      else if( spliceElement.isMerge() )
        rhsDuct = createMergeStage( (Merge) element, role );
      else
        rhsDuct = createHashJoinGate( (HashJoin) element );
      }
    else if( element instanceof Tap )
      {
      rhsDuct = createSinkStage( (Tap) element );
      }
    else
      throw new IllegalStateException( "unknown element type: " + element.getClass().getName() );

    return rhsDuct;
    }

  protected Duct createBoundaryStage( Boundary element, IORole role )
    {
    // could return MergeStage at this point as they are roughly equivalent
    throw new UnsupportedOperationException( "boundary not supported by planner" );
    }

  protected SinkStage createSinkStage( Tap element )
    {
    return new SinkStage( flowProcess, element );
    }

  protected abstract Gate createCoGroupGate( CoGroup element, IORole role );

  protected abstract Gate createGroupByGate( GroupBy element, IORole role );

  protected Duct createMergeStage( Merge merge, IORole both )
    {
    return new MergeStage( flowProcess, merge );
    }

  protected Gate createHashJoinGate( HashJoin join )
    {
    if( join.getNumSelfJoins() != 0 )
      return createBlockingJoinGate( join );

    // lets not block the streamed side unless it will cause a deadlock
    if( hasElementAnnotation( BlockingMode.Blocked, join ) )
      return createBlockingJoinGate( join );

    return createNonBlockingJoinGate( join );
    }

  private boolean hasElementAnnotation( Enum annotation, FlowElement flowElement )
    {
    if( !( (AnnotatedGraph) elementGraph ).hasAnnotations() )
      return false;

    return ( (AnnotatedGraph) elementGraph ).getAnnotations().hadKey( annotation, flowElement );
    }

  protected GroupingSpliceGate createNonBlockingJoinGate( HashJoin join )
    {
    return new MemoryHashJoinGate( flowProcess, join );
    }

  protected MemoryCoGroupGate createBlockingJoinGate( HashJoin join )
    {
    return new MemoryCoGroupGate( flowProcess, join );
    }

  protected Duct findExisting( Duct current )
    {
    Collection<Duct> allDucts = getAllDucts();

    for( Duct duct : allDucts )
      {
      if( duct.equals( current ) )
        return duct;
      }

    return current;
    }

  protected void setTraps()
    {
    Collection<Duct> ducts = getAllDucts();

    for( Duct duct : ducts )
      {
      if( !( duct instanceof ElementDuct ) )
        continue;

      ElementDuct elementDuct = (ElementDuct) duct;
      FlowElement flowElement = elementDuct.getFlowElement();

      Set<String> branchNames = new TreeSet<String>();

      if( flowElement instanceof Pipe )
        branchNames.add( ( (Pipe) flowElement ).getName() );
      else if( flowElement instanceof Tap )
        branchNames.addAll( getTapBranchNamesFor( duct ) );
      else
        throw new IllegalStateException( "unexpected duct type" + duct.getClass().getCanonicalName() );

      elementDuct.setBranchNames( branchNames );

      for( String branchName : branchNames )
        {
        Tap trap = node.getTrap( branchName );

        if( trap != null )
          {
          FlowProcess elementFlowProcess = new ElementFlowProcess( flowProcess, trap.getConfigDef() );
          elementDuct.setTrapHandler( new TrapHandler( elementFlowProcess, flowElement, trap, branchName ) );
          break;
          }
        }

      if( !elementDuct.hasTrapHandler() )
        elementDuct.setTrapHandler( new TrapHandler( flowProcess ) );
      }
    }

  /**
   * Returns a Set as a given tap may be bound to multiple branches
   *
   * @param duct
   * @return
   */
  private Set<String> getTapBranchNamesFor( Duct duct )
    {
    if( ( (Tap) ( (ElementDuct) duct ).getFlowElement() ).isTemporary() )
      return Collections.emptySet();

    if( duct instanceof SourceStage )
      return node.getSourceTapNames( (Tap) ( (SourceStage) duct ).getFlowElement() );
    else if( duct instanceof SinkStage )
      return node.getSinkTapNames( (Tap) ( (SinkStage) duct ).getFlowElement() );
    else
      throw new IllegalStateException( "duct does not wrap a Tap: " + duct.getClass().getCanonicalName() );
    }

  protected void setScopes()
    {
    Collection<Duct> ducts = getAllDucts();

    for( Duct duct : ducts )
      {
      if( !( duct instanceof ElementDuct ) )
        continue;

      ElementDuct elementDuct = (ElementDuct) duct;

      // get the actual incoming/outgoing scopes for the full node as we need the total number of branches
      elementDuct.getIncomingScopes().addAll( node.getPreviousScopes( elementDuct.getFlowElement() ) );
      elementDuct.getOutgoingScopes().addAll( node.getNextScopes( elementDuct.getFlowElement() ) );
      }
    }
  }
