/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import cascading.util.jgrapht.DOTExporter;
import cascading.util.jgrapht.IntegerNameProvider;
import cascading.util.jgrapht.StringEdgeNameProvider;
import cascading.util.jgrapht.StringNameProvider;
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

  private final SearchOrder searchOrder;
  private final DirectedMultigraph<ElementExpression, ScopeExpression> graph;

  private boolean allowNonRecursiveMatching;

  public ExpressionGraph()
    {
    this.searchOrder = SearchOrder.ReverseTopological;
    this.graph = new DirectedMultigraph( new ClassBasedEdgeFactory( PathScopeExpression.class ) );
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
    this.graph = new DirectedMultigraph( new ClassBasedEdgeFactory( PathScopeExpression.class ) );
    this.allowNonRecursiveMatching = allowNonRecursiveMatching;
    }

  public DirectedMultigraph<ElementExpression, ScopeExpression> getGraph()
    {
    return graph;
    }

  public SearchOrder getSearchOrder()
    {
    return searchOrder;
    }

  public boolean supportsNonRecursiveMatch()
    {
    return allowNonRecursiveMatching &&
      getGraph().vertexSet().size() == 1 &&
      Util.getFirst( getGraph().vertexSet() ).getCapture() == ElementCapture.Primary;
    }

  public ExpressionGraph setAllowNonRecursiveMatching( boolean allowNonRecursiveMatching )
    {
    this.allowNonRecursiveMatching = allowNonRecursiveMatching;

    return this;
    }

  public ExpressionGraph arcs( ElementExpression... matchers )
    {
    ElementExpression lhs = null;

    for( ElementExpression matcher : matchers )
      {
      graph.addVertex( matcher );

      if( lhs != null )
        graph.addEdge( lhs, matcher );

      lhs = matcher;
      }

    return this;
    }

  public ExpressionGraph arc( ElementExpression lhsMatcher, ScopeExpression scopeMatcher, ElementExpression rhsMatcher )
    {
    graph.addVertex( lhsMatcher );
    graph.addVertex( rhsMatcher );

    // can never re-use edges, must be wrapped
    graph.addEdge( lhsMatcher, rhsMatcher, new DelegateScopeExpression( scopeMatcher ) );

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

      new DOTExporter( new IntegerNameProvider(), new StringNameProvider(), new StringEdgeNameProvider() ).export( writer, getGraph() );

      writer.close();

      Util.writePDF( filename );
      }
    catch( IOException exception )
      {
      LOG.error( "failed printing expression graph to: {}, with exception: {}", filename, exception );
      }
    }

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
    public boolean isCapture()
      {
      return delegate.isCapture();
      }

    @Override
    public boolean acceptsAll()
      {
      return delegate.acceptsAll();
      }

    @Override
    public boolean appliesToAllPaths()
      {
      return delegate.appliesToAllPaths();
      }

    @Override
    public boolean appliesToAnyPath()
      {
      return delegate.appliesToAnyPath();
      }

    @Override
    public boolean appliesToEachPath()
      {
      return delegate.appliesToEachPath();
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
  }
