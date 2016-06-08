/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream.element;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.flow.stream.duct.Stage;
import cascading.flow.stream.graph.IORole;
import cascading.pipe.Boundary;
import cascading.pipe.Pipe;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class BoundaryStage<Incoming, Outgoing> extends Stage<Incoming, Outgoing> implements ElementDuct
  {
  protected Boundary boundary;
  protected final FlowProcess flowProcess;
  protected IORole role = IORole.both;

  protected final List<Scope> incomingScopes = new ArrayList<>();
  protected final List<Scope> outgoingScopes = new ArrayList<>();

  private TrapHandler trapHandler;
  private Set<String> branchNames;

  public BoundaryStage( FlowProcess flowProcess, Boundary boundary )
    {
    this.boundary = boundary;

    Pipe element = boundary;

    while( element != null )
      {
      if( element.hasConfigDef() )
        flowProcess = new ElementFlowProcess( flowProcess, element.getConfigDef() );

      element = element.getParent();
      }

    this.flowProcess = flowProcess;
    }

  public BoundaryStage( FlowProcess flowProcess, Boundary boundary, IORole role )
    {
    this.boundary = boundary;
    this.flowProcess = flowProcess;
    this.role = role;
    }

  public Boundary getBoundary()
    {
    return boundary;
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

  @Override
  public FlowElement getFlowElement()
    {
    return boundary;
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
  }
