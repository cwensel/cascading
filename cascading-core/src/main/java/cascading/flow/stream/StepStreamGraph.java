/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.BaseFlowStep;
import cascading.flow.planner.Scope;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tap.Tap;
import org.jgrapht.GraphPath;

import static cascading.flow.planner.ElementGraphs.getAllShortestPathsBetween;

/**
 *
 */
public abstract class StepStreamGraph extends StreamGraph
  {
  protected FlowProcess flowProcess;
  protected final BaseFlowStep step;

  public StepStreamGraph( FlowProcess flowProcess, BaseFlowStep step )
    {
    this.flowProcess = flowProcess;
    this.step = step;
    }

  protected Object getProperty( String name )
    {
    return flowProcess.getProperty( name );
    }

  protected void handleDuct( FlowElement lhsElement, Duct lhsDuct )
    {
    List<FlowElement> successors = step.getSuccessors( lhsElement );

    if( !stopOnElement( lhsElement, successors ) )
      handleSuccessors( lhsDuct, successors );
    else
      addTail( lhsDuct );
    }

  protected abstract boolean stopOnElement( FlowElement lhsElement, List<FlowElement> successors );

  private void handleSuccessors( Duct lhsDuct, List<FlowElement> successors )
    {
    for( FlowElement rhsElement : successors )
      {
      Duct newRhsDuct = createDuctFor( rhsElement );
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
    if( !( rhsDuct instanceof SpliceGate ) )
      return 0;

    FlowElement lhsElement = ( (ElementDuct) lhsDuct ).getFlowElement();
    Splice rhsElement = (Splice) ( (SpliceGate) rhsDuct ).getFlowElement();

    List<GraphPath<FlowElement, Scope>> paths = getAllShortestPathsBetween( step.getGraph(), lhsElement, rhsElement );

    for( GraphPath<FlowElement, Scope> path : paths )
      {
      if( path.getEdgeList().size() == 1 )
        return rhsElement.getPipePos().get( path.getEdgeList().get( 0 ).getName() );
      }

    throw new IllegalStateException( "could not find ordinal" );
    }

  private Duct createDuctFor( FlowElement element )
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
    else if( element instanceof Splice )
      {
      Splice spliceElement = (Splice) element;

      if( spliceElement.isGroupBy() )
        rhsDuct = createGroupByGate( (GroupBy) spliceElement );
      else if( spliceElement.isCoGroup() )
        rhsDuct = createCoGroupGate( (CoGroup) spliceElement );
      else if( spliceElement.isMerge() )
        rhsDuct = createMergeStage( (Merge) element );
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

  protected SinkStage createSinkStage( Tap element )
    {
    return new SinkStage( flowProcess, element );
    }

  protected abstract Gate createCoGroupGate( CoGroup element );

  protected abstract Gate createGroupByGate( GroupBy element );

  protected Duct createMergeStage( Merge merge )
    {
    return new MergeStage( flowProcess, merge );
    }

  protected Gate createHashJoinGate( HashJoin join )
    {
    if( join.getNumSelfJoins() != 0 )
      return createBlockingJoinGate( join );

    // lets not block the streamed side unless it will cause a deadlock
    if( joinHasSameStreamedSource( join ) )
      return createBlockingJoinGate( join );

    return createNonBlockingJoinGate( join );
    }

  protected MemoryHashJoinGate createNonBlockingJoinGate( HashJoin join )
    {
    return new MemoryHashJoinGate( flowProcess, join );
    }

  protected MemoryCoGroupGate createBlockingJoinGate( HashJoin join )
    {
    return new MemoryCoGroupGate( flowProcess, join );
    }

  private boolean joinHasSameStreamedSource( HashJoin join )
    {
    if( !step.getStreamedSourceByJoin().isEmpty() )
      {
      // if streamed source is multi path
      Object tap = step.getStreamedSourceByJoin().get( join );

      return getNumImmediateBranches( (FlowElement) tap, join ) > 1;
      }

    // means we are in local mode if joins is empty
    for( Object tap : step.getSources() )
      {
      if( getNumImmediateBranches( (FlowElement) tap, join ) > 1 )
        return true;
      }

    return false;
    }

  private int getNumImmediateBranches( FlowElement tap, HashJoin join )
    {
    return getAllShortestPathsBetween( step.getGraph(), tap, join ).size();
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
        Tap trap = step.getTrap( branchName );

        if( trap != null )
          {
          elementDuct.setTrapHandler( new TrapHandler( flowProcess, trap, branchName ) );
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
    if( duct instanceof SourceStage )
      return step.getSourceName( (Tap) ( (SourceStage) duct ).getFlowElement() );
    else if( duct instanceof SinkStage )
      return step.getSinkName( (Tap) ( (SinkStage) duct ).getFlowElement() );
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

      elementDuct.getIncomingScopes().addAll( step.getPreviousScopes( elementDuct.getFlowElement() ) );
      elementDuct.getOutgoingScopes().addAll( step.getNextScopes( elementDuct.getFlowElement() ) );
      }
    }
  }
