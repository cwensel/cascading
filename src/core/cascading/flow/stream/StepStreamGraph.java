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

package cascading.flow.stream;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.FlowStep;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;

/**
 *
 */
public abstract class StepStreamGraph extends StreamGraph
  {
  protected final FlowProcess flowProcess;
  protected final FlowStep step;

  public StepStreamGraph( FlowProcess flowProcess, FlowStep step )
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
      Duct rhsDuct = createDuctFor( rhsElement );
      Duct newRhsDuct = findExisting( rhsDuct );

      addPath( lhsDuct, newRhsDuct );

      if( rhsDuct != newRhsDuct ) // don't keep going if we have already seen rhs
        return;

      handleDuct( rhsElement, rhsDuct );
      }
    }

  private Duct createDuctFor( FlowElement element )
    {
    Duct rhsDuct = null;

    if( element instanceof Each )
      {
      if( ( (Each) element ).isFunction() )
        rhsDuct = new FunctionEachStage( flowProcess, (Each) element );
      else if( ( (Each) element ).isFilter() )
        rhsDuct = new FilterEachStage( flowProcess, (Each) element );
      else if( ( (Each) element ).isValueAssertion() )
        rhsDuct = new ValueAssertionEachStage( flowProcess, (Each) element );
      else
        throw new IllegalStateException( "unknown operation: " + ( (Each) element ).getOperation().getClass().getCanonicalName() );
      }
    else if( element instanceof Every )
      {
      if( ( (Every) element ).isBuffer() )
        rhsDuct = new BufferEveryWindow( flowProcess, (Every) element );
      else if( ( (Every) element ).isAggregator() )
        rhsDuct = new AggregatorEveryStage( flowProcess, (Every) element );
      else if( ( (Every) element ).isGroupAssertion() )
        rhsDuct = new GroupAssertionEveryStage( flowProcess, (Every) element );
      else
        throw new IllegalStateException( "unknown operation: " + ( (Every) element ).getOperation().getClass().getCanonicalName() );
      }
    else if( element instanceof Group )
      {
      if( ( (Group) element ).isGroupBy() )
        rhsDuct = createGroupByGate( (Group) element );
      else
        rhsDuct = createCoGroupGate( (Group) element );
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
    return new SinkStage( flowProcess, (Tap) element );
    }

  protected abstract Gate createCoGroupGate( Group element );

  protected abstract Gate createGroupByGate( Group element );

  private Duct findExisting( Duct current )
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

//  private Set<String> makeNames( Set<Scope> nextScopes )
//    {
//    Set<String> names = new HashSet<String>();
//
//    for( Scope scope : nextScopes )
//      names.add( scope.getName() );
//
//    return names;
//    }

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
