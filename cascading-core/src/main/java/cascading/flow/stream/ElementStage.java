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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
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
    this.flowElement = flowElement;

    FlowElement element = flowElement;

    while( element != null )
      {
      if( element.hasConfigDef() )
        flowProcess = new ElementFlowProcess( flowProcess, element.getConfigDef() );

      if( element instanceof Pipe )
        element = ( (Pipe) element ).getParent();
      else
        element = null;
      }

    this.flowProcess = flowProcess;
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
    return unwind( next ).getFlowElement().resolveIncomingOperationPassThroughFields( outgoingScopes.get( 0 ) );
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
