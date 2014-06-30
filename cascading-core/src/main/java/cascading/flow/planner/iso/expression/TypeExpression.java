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

package cascading.flow.planner.iso.expression;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.graph.ElementGraph;

/**
 *
 */
public class TypeExpression<Type> extends ElementExpression
  {
  public enum Topo
    {
      Ignore,
      Head,
      Tail,
      Linear,
      LinearIn,
      LinearOut,
      Splice,
      Split,
      SpliceSplit
    }

  boolean exact = false;
  Class<? extends Type> type;
  Topo topo = Topo.Ignore;

  public TypeExpression( ElementCapture capture, boolean exact, Class<? extends Type> type, Topo topo )
    {
    super( capture );
    this.exact = exact;
    this.type = type;
    this.topo = topo;
    }

  public TypeExpression( ElementCapture capture, boolean exact, Class<? extends Type> type )
    {
    super( capture );
    this.exact = exact;
    this.type = type;
    }

  public TypeExpression( ElementCapture capture, Class<? extends Type> type, Topo topo )
    {
    super( capture );
    this.type = type;
    this.topo = topo;
    }

  public TypeExpression( ElementCapture capture, Class<? extends Type> type )
    {
    super( capture );
    this.type = type;
    }

  public TypeExpression( boolean exact, Class<? extends Type> type, Topo topo )
    {
    this.exact = exact;
    this.type = type;
    this.topo = topo;
    }

  public TypeExpression( boolean exact, Class<? extends Type> type )
    {
    this.exact = exact;
    this.type = type;
    }

  public TypeExpression( Class<? extends Type> type, Topo topo )
    {
    this.type = type;
    this.topo = topo;
    }

  public TypeExpression( Class<? extends Type> type )
    {
    this.type = type;
    }

  protected Class<? extends Type> getType( FlowElement flowElement )
    {
    return (Class<? extends Type>) flowElement.getClass();
    }

  @Override
  public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, FlowElement flowElement )
    {
    boolean typeApplies = typeApplies( flowElement );

    if( !typeApplies )
      return false;

    if( topo == Topo.Ignore )
      return true;

    // todo: make lazy
    boolean isHead = elementGraph.inDegreeOf( flowElement ) == 0;
    boolean isTail = elementGraph.outDegreeOf( flowElement ) == 0;
    boolean isSplice = elementGraph.inDegreeOf( flowElement ) > 1;
    boolean isSplit = elementGraph.outDegreeOf( flowElement ) > 1;

    switch( topo )
      {
      case Head:
        return isHead;
      case Tail:
        return isTail;
      case Linear:
        return !isSplice && !isSplit;
      case LinearIn:
        return !isSplice;
      case LinearOut:
        return !isSplit;
      case Splice:
        return isSplice && !isSplit;
      case Split:
        return !isSplice && isSplit;
      case SpliceSplit:
        return isSplice && isSplit;
      }

    throw new IllegalStateException( "unknown switch, got: " + topo );
    }


  private boolean typeApplies( FlowElement flowElement )
    {
    Class<? extends Type> givenType = getType( flowElement );

    if( givenType == null )
      return false;

    if( exact )
      return givenType == type;

    return type.isAssignableFrom( givenType );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( getClass().getSimpleName() ).append( "{" );
    sb.append( "exact=" ).append( exact );
    sb.append( ", type=" ).append( type );
    sb.append( ", topo=" ).append( topo );
    sb.append( '}' );
    return sb.toString();
    }
  }
