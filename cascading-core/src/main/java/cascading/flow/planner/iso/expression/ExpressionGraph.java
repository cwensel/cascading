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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.finder.SearchOrder;
import cascading.util.Util;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.IntegerNameProvider;
import org.jgrapht.ext.StringEdgeNameProvider;
import org.jgrapht.ext.StringNameProvider;
import org.jgrapht.graph.ClassBasedEdgeFactory;
import org.jgrapht.graph.DirectedMultigraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ExpressionGraph
  {
  private static final Logger LOG = LoggerFactory.getLogger( ExpressionGraph.class );

  public static ScopeExpression unwind( ScopeExpression scopeExpression )
    {
    if( scopeExpression instanceof DelegateScopeExpression )
      return ( (DelegateScopeExpression) scopeExpression ).delegate;

    return scopeExpression;
    }

  private static class DelegateScopeExpression extends ScopeExpression
    {
    ScopeExpression delegate;

    protected DelegateScopeExpression( ScopeExpression delegate )
      {
      this.delegate = delegate;
      }

    @Override
    public boolean acceptsAll()
      {
      return delegate.acceptsAll();
      }

    @Override
    public boolean applies( PlannerContext plannerContext, ElementGraph elementGraph, Scope scope )
      {
      return delegate.applies( plannerContext, elementGraph, scope );
      }

    @Override
    public String toString()
      {
      return delegate.toString();
      }
    }

  private final SearchOrder searchOrder;
  private final DirectedMultigraph<ElementExpression, ScopeExpression> delegate;

  private boolean allowNonRecursiveMatching;

  public ExpressionGraph()
    {
    this.searchOrder = SearchOrder.ReverseTopological;
    this.delegate = new DirectedMultigraph( new ClassBasedEdgeFactory( PathScopeExpression.class ) );
    this.allowNonRecursiveMatching = true;
    }

  public ExpressionGraph( boolean allowNonRecursiveMatching )
    {
    this();
    this.allowNonRecursiveMatching = allowNonRecursiveMatching;
    }

  public ExpressionGraph( ElementExpression... matchers )
    {
    this();
    arcs( matchers );
    }

  public ExpressionGraph( SearchOrder searchOrder, ElementExpression... matchers )
    {
    this( searchOrder );
    arcs( matchers );
    }

  public ExpressionGraph( SearchOrder searchOrder )
    {
    this( searchOrder, true );
    }

  public ExpressionGraph( SearchOrder searchOrder, boolean allowNonRecursiveMatching )
    {
    this.searchOrder = searchOrder;
    this.delegate = new DirectedMultigraph( new ClassBasedEdgeFactory( PathScopeExpression.class ) );
    this.allowNonRecursiveMatching = allowNonRecursiveMatching;
    }

  public DirectedMultigraph<ElementExpression, ScopeExpression> getDelegate()
    {
    return delegate;
    }

  public SearchOrder getSearchOrder()
    {
    return searchOrder;
    }

  public boolean supportsNonRecursiveMatch()
    {
    return allowNonRecursiveMatching && !searchOrder.isReversed() && // if reversed is requested, the transform must be recursive
      getDelegate().vertexSet().size() == 1 &&
      Util.getFirst( getDelegate().vertexSet() ).getCapture() == ElementExpression.Capture.Primary;
    }

  public ExpressionGraph arcs( ElementExpression... matchers )
    {
    ElementExpression lhs = null;

    for( ElementExpression matcher : matchers )
      {
      delegate.addVertex( matcher );

      if( lhs != null )
        delegate.addEdge( lhs, matcher );

      lhs = matcher;
      }

    return this;
    }

  public ExpressionGraph arc( ElementExpression lhsMatcher, ScopeExpression scopeMatcher, ElementExpression rhsMatcher )
    {
    delegate.addVertex( lhsMatcher );
    delegate.addVertex( rhsMatcher );

    // can never re-use edges, must be wrapped
    delegate.addEdge( lhsMatcher, rhsMatcher, new DelegateScopeExpression( scopeMatcher ) );

    return this;
    }

  public void writeDOT( String filename )
    {
    try
      {
      File parentFile = new File( filename ).getParentFile();

      if( parentFile != null && !parentFile.exists() )
        parentFile.mkdirs();

      Writer writer = new FileWriter( filename );

      new DOTExporter( new IntegerNameProvider(), new StringNameProvider(), new StringEdgeNameProvider() ).export( writer, getDelegate() );

      writer.close();

      Util.writePDF( filename );
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing expression graph to: {}, with exception: {}", filename, exception );
      }
    }
  }
