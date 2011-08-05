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
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class ElementStage<Incoming, Outgoing> extends Stage<Incoming, Outgoing> implements ElementDuct
  {
  protected final FlowProcess flowProcess;
  protected final FlowElement flowElement;
  protected Set<String> branchNames;
  protected TrapHandler trapHandler;

  protected final List<Scope> incomingScopes = new ArrayList<Scope>();
  protected final List<Scope> outgoingScopes = new ArrayList<Scope>();

  public ElementStage( FlowProcess flowProcess, FlowElement flowElement )
    {
    this.flowProcess = flowProcess;
    this.flowElement = flowElement;
    }

  public FlowElement getFlowElement()
    {
    return flowElement;
    }

  @Override
  public List<Scope> getIncomingScopes()
    {
    return incomingScopes;
    }

  public Set<String> getBranchNames()
    {
    return branchNames;
    }

  public void setBranchNames( Set<String> branchNames )
    {
    this.branchNames = branchNames;
    }

  public void setTrapHandler( TrapHandler trapHandler )
    {
    this.trapHandler = trapHandler;
    }

  @Override
  public boolean hasTrapHandler()
    {
    return trapHandler != null;
    }

  public void addIncomingScope( Scope incomingScope )
    {
    incomingScopes.add( incomingScope );
    }

  @Override
  public List<Scope> getOutgoingScopes()
    {
    return outgoingScopes;
    }

  public void addOutgoingScope( Scope outgoingScope )
    {
    outgoingScopes.add( outgoingScope );
    }

  protected Fields getOutgoingFields()
    {
    return unwind( next ).getFlowElement().resolveFields( outgoingScopes.get( 0 ) );
    }

  private ElementDuct unwind( Duct next )
    {
    if( next instanceof ElementDuct )
      return (ElementDuct) next;

    return unwind( next.getNext() );
    }

  @Override
  public void cleanup()
    {
    super.cleanup();

    // close if top of stack
    if( next == null )
      TrapHandler.closeTraps();
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
  public final boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof ElementStage ) )
      return false;

    ElementStage that = (ElementStage) object;

    if( flowElement != null ? flowElement != that.flowElement : that.flowElement != null )
      return false;

    return true;
    }

  @Override
  public final int hashCode()
    {
    return flowElement != null ? System.identityHashCode( flowElement ) : 0;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( getClass().getSimpleName() );
    sb.append( "{flowElement=" ).append( flowElement );
    sb.append( '}' );
    return sb.toString();
    }
  }
