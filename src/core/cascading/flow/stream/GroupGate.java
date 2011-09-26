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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.pipe.Group;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryIterator;

/**
 *
 */
public abstract class GroupGate extends Gate<TupleEntry, Grouping<TupleEntry, TupleEntryIterator>> implements ElementDuct
  {
  protected Duct[] orderedPrevious;

  public enum Role
    {
      sink, source, both
    }

  protected final FlowProcess flowProcess;
  protected Role role = Role.both;

  private TrapHandler trapHandler;
  private Set<String> branchNames;

  protected final Group group;
  protected final List<Scope> incomingScopes = new ArrayList<Scope>();
  protected final List<Scope> outgoingScopes = new ArrayList<Scope>();
  protected Fields[] groupFields;
  protected Fields[] sortFields;
  protected Fields[] valuesFields;


  protected Grouping<TupleEntry, TupleEntryIterator> grouping;
  protected TupleEntryChainIterator tupleEntryIterator;
  protected TupleEntry groupingEntry;

  public GroupGate( FlowProcess flowProcess, Group group )
    {
    this.group = group;
    this.flowProcess = flowProcess;
    }

  public GroupGate( FlowProcess flowProcess, Group group, Role role )
    {
    this.group = group;
    this.flowProcess = flowProcess;
    this.role = role;
    }

  public void setBranchNames( Set<String> branchNames )
    {
    this.branchNames = branchNames;
    }

  public Set<String> getBranchNames()
    {
    return branchNames;
    }

  @Override
  public void setTrapHandler( TrapHandler trapHandler )
    {
    this.trapHandler = trapHandler;
    }

  @Override
  public boolean hasTrapHandler()
    {
    return trapHandler != null;
    }

  protected void handleReThrowableException( String message, Throwable throwable )
    {
    trapHandler.handleReThrowableException( message, throwable );
    }

  protected void handleException( Throwable exception, TupleEntry tupleEntry )
    {
    trapHandler.handleException( exception, tupleEntry );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    if( incomingScopes.size() == 0 )
      throw new IllegalStateException( "incoming scopes may not be empty" );

    if( outgoingScopes.size() == 0 )
      throw new IllegalStateException( "outgoing scope may not be empty" );

    groupFields = new Fields[ incomingScopes.size() ];
    valuesFields = new Fields[ incomingScopes.size() ];

    if( group.isSorted() )
      sortFields = new Fields[ incomingScopes.size() ];

    for( Scope incomingScope : incomingScopes )
      {
      int pos = group.getPipePos().get( incomingScope.getName() );

      groupFields[ pos ] = outgoingScopes.get( 0 ).getGroupingSelectors().get( incomingScope.getName() );
      valuesFields[ pos ] = incomingScope.getOutValuesFields();

      if( sortFields != null )
        sortFields[ pos ] = outgoingScopes.get( 0 ).getSortingSelectors().get( incomingScope.getName() );
      }

    if( role == Role.sink )
      return;

    groupingEntry = new TupleEntry( outgoingScopes.get( 0 ).getOutGroupingFields(), true );
    tupleEntryIterator = new TupleEntryChainIterator( outgoingScopes.get( 0 ).getOutValuesFields() );

    grouping = new Grouping<TupleEntry, TupleEntryIterator>();
    grouping.group = groupingEntry;
    grouping.iterator = tupleEntryIterator;
    }

  @Override
  public FlowElement getFlowElement()
    {
    return group;
    }

  @Override
  public List<Scope> getOutgoingScopes()
    {
    return outgoingScopes;
    }

  @Override
  public List<Scope> getIncomingScopes()
    {
    return incomingScopes;
    }

  public void addIncomingScope( Scope incomingScope )
    {
    incomingScopes.add( incomingScope );
    }

  public void addOutgoingScope( Scope outgoingScope )
    {
    outgoingScopes.add( outgoingScope );
    }

  @Override
  public void cleanup()
    {
    super.cleanup();

    // close if top of stack
    if( next == null )
      TrapHandler.closeTraps();
    }

  protected synchronized void orderDucts()
    {
    orderedPrevious = new Duct[ incomingScopes.size() ];

    for( int i = 0; i < group.getPrevious().length; i++ )
      {
      Pipe next = group.getPrevious()[ i ];

      int[] depth = new int[ allPrevious.length ];

      for( int j = 0; j < allPrevious.length; j++ )
        depth[ j ] = findName( 0, (ElementDuct) allPrevious[ j ], next );

      int lastDepth = Integer.MAX_VALUE;

      for( int j = 0; j < depth.length; j++ )
        {
        if( lastDepth == depth[ j ] && lastDepth != Integer.MAX_VALUE )
          throw new IllegalStateException( "two branches with same depth" );

        if( lastDepth > depth[ j ] )
          {
          orderedPrevious[ i ] = allPrevious[ j ];
          lastDepth = depth[ j ];
          }
        }
      }
    }

  private int findName( int count, ElementDuct duct, Pipe next )
    {
    FlowElement flowElement = ( (ElementDuct) duct ).getFlowElement();

    if( next == flowElement )
      return count;

    if( flowElement instanceof Tap && duct.getBranchNames().contains( next.getName() ) )
      return count + 1;

    Pipe[] previous = next.getPrevious();

    if( previous.length != 1 )
      return Integer.MAX_VALUE; // there is a merge

    return findName( ++count, duct, previous[ 0 ] );
    }

  protected void makePosMap( Map<Duct, Integer> posMap )
    {
    for( int i = 0; i < orderedPrevious.length; i++ )
      {
      if( orderedPrevious[ i ] != null )
        posMap.put( orderedPrevious[ i ], i );
      }
    }

  @Override
  public final boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof GroupGate ) )
      return false;

    GroupGate groupGate = (GroupGate) object;

    if( group != null ? group != groupGate.group : groupGate.group != null )
      return false;

    return true;
    }

  @Override
  public final int hashCode()
    {
    return group != null ? System.identityHashCode( group ) : 0;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( getClass().getSimpleName() );
    sb.append( "{group=" ).append( group );
    sb.append( '}' );
    return sb.toString();
    }
  }
