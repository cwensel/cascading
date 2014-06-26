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

package cascading.flow.planner.iso.finder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import cascading.flow.FlowElement;
import cascading.flow.planner.PlannerContext;
import cascading.flow.planner.Scope;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.iso.expression.ElementExpression;
import cascading.flow.planner.iso.expression.Expression;
import cascading.flow.planner.iso.expression.ScopeExpression;
import cascading.util.Pair;
import org.jgrapht.DirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class and algorithm is based on the following research.
 * <p/>
 * Cordella, L. P., Foggia, P., Sansone, C., & VENTO, M. (2004).
 * A (sub)graph isomorphism algorithm for matching large graphs.
 * IEEE Transactions on Pattern Analysis and Machine Intelligence, 26(10), 1367–1372.
 * doi:10.1109/TPAMI.2004.75
 * <p/>
 * Implementation in Java, modifications, and bug fixes have been applied.
 * <p/>
 * Keep variable/field naming schemes mirroring the above paper to improve/retain readability.
 * <p/>
 * Notes:
 * <p/>
 * The #areCompatibleEdges does not scale, but it isn't conceived there will be more than a few edges
 * in any given node pair.
 * <p/>
 * It also accounts for a wild card stating any edge between the nodes is relevant.
 * <p/>
 * Future versions need the ability to specifically match and capture an edge, or all edges that apply to
 * an expression.
 */
class State
  {
  private static final Logger LOG = LoggerFactory.getLogger( State.class );

  public static final int NULL_NODE = -1;

  private FinderContext finderContext;
  private PlannerContext plannerContext;

  private IndexedMatchGraph matchGraph;
  private IndexedElementGraph elementGraph;

  /** The number of nodes currently being matched between g1 and g3 */
  int coreLen;

  /**
   * The number of nodes that were matched prior to this current pair being
   * added, which is used in backtracking.
   */
  int origCoreLen;

  /** The node in g1 that was most recently added. */
  int addedNode1;

  // State information
  int t1bothLen;
  int t2bothLen;
  int t1inLen;
  int t1outLen;
  int t2inLen;
  int t2outLen;

  int[] core1;
  int[] core2;
  int[] in1;
  int[] in2;
  int[] out1;
  int[] out2;

  int[] order;

  /** The number of nodes in {@code matchedGraph} */
  private final int n1;

  /** The number of nodes in {@code elementGraph} */
  private final int n2;

  State( FinderContext finderContext, PlannerContext plannerContext, SearchOrder searchOrder, DirectedGraph<ElementExpression, ScopeExpression> matchGraph, ElementGraph elementGraph )
    {
    this.finderContext = finderContext;
    this.plannerContext = plannerContext;
    this.matchGraph = new IndexedMatchGraph( matchGraph );
    this.elementGraph = new IndexedElementGraph( searchOrder, elementGraph );

    n1 = matchGraph.vertexSet().size();
    n2 = elementGraph.vertexSet().size();

    order = null;

    coreLen = 0;
    origCoreLen = 0;
    t1bothLen = 0;
    t1inLen = 0;
    t1outLen = 0;
    t2bothLen = 0;
    t2inLen = 0;
    t2outLen = 0;

    addedNode1 = NULL_NODE;

    core1 = new int[ n1 ];
    core2 = new int[ n2 ];
    in1 = new int[ n1 ];
    in2 = new int[ n2 ];
    out1 = new int[ n1 ];
    out2 = new int[ n2 ];

    Arrays.fill( core1, NULL_NODE );
    Arrays.fill( core2, NULL_NODE );
    }

  protected State( State copy )
    {
    finderContext = copy.finderContext;
    plannerContext = copy.plannerContext;
    matchGraph = copy.matchGraph;
    elementGraph = copy.elementGraph;

    coreLen = copy.coreLen;
    origCoreLen = copy.coreLen; // sets orig to copy
    t1bothLen = copy.t1bothLen;
    t2bothLen = copy.t2bothLen;
    t1inLen = copy.t1inLen;
    t2inLen = copy.t2inLen;
    t1outLen = copy.t1outLen;
    t2outLen = copy.t2outLen;
    n1 = copy.n1;
    n2 = copy.n2;

    addedNode1 = NULL_NODE;

    core1 = copy.core1;
    core2 = copy.core2;
    in1 = copy.in1;
    in2 = copy.in2;
    out1 = copy.out1;
    out2 = copy.out2;
    order = copy.order;
    }

  public Pair<Integer, Integer> nextPair( int prevN1, int prevN2 )
    {
    if( prevN1 == NULL_NODE )
      prevN1 = 0;

    if( prevN2 == NULL_NODE )
      prevN2 = 0;
    else
      prevN2++;

    if( t1bothLen > coreLen && t2bothLen > coreLen )
      {
      while( prevN1 < n1 && ( core1[ prevN1 ] != NULL_NODE || out1[ prevN1 ] == 0 || in1[ prevN1 ] == 0 ) )
        {
        prevN1++;
        prevN2 = 0;
        }
      }
    else if( t1outLen > coreLen && t2outLen > coreLen )
      {
      while( prevN1 < n1 && ( core1[ prevN1 ] != NULL_NODE || out1[ prevN1 ] == 0 ) )
        {
        prevN1++;
        prevN2 = 0;
        }
      }
    else if( t1inLen > coreLen && t2inLen > coreLen )
      {
      while( prevN1 < n1 && ( core1[ prevN1 ] != NULL_NODE || in1[ prevN1 ] == 0 ) )
        {
        prevN1++;
        prevN2 = 0;
        }
      }
    else if( prevN1 == 0 && order != null )
      {
      int i = 0;

      while( i < n1 && core1[ prevN1 = order[ i ] ] != NULL_NODE )
        i++;

      if( i == n1 )
        prevN1 = n1;
      }
    else
      {
      while( prevN1 < n1 && core1[ prevN1 ] != NULL_NODE )
        {
        prevN1++;
        prevN2 = 0;
        }
      }

    if( t1bothLen > coreLen && t2bothLen > coreLen )
      {
      while( prevN2 < n2 && ( core2[ prevN2 ] != NULL_NODE || out2[ prevN2 ] == 0 || in2[ prevN2 ] == 0 ) )
        prevN2++;
      }
    else if( t1outLen > coreLen && t2outLen > coreLen )
      {
      while( prevN2 < n2 && ( core2[ prevN2 ] != NULL_NODE || out2[ prevN2 ] == 0 ) )
        prevN2++;
      }
    else if( t1inLen > coreLen && t2inLen > coreLen )
      {
      while( prevN2 < n2 && ( core2[ prevN2 ] != NULL_NODE || in2[ prevN2 ] == 0 ) )
        prevN2++;
      }
    else
      {
      while( prevN2 < n2 && core2[ prevN2 ] != NULL_NODE )
        prevN2++;
      }

    LOG.debug( "prevN1: {}, prevN2: {}", prevN1, prevN2 );

    if( prevN1 < n1 && prevN2 < n2 )
      return new Pair<>( prevN1, prevN2 );
    else
      return null;
    }

  protected boolean areCompatibleEdges( int v1, int v2, int v3, int v4 )
    {
    // there is probably a more elegant solution
    List<ScopeExpression> matchers = matchGraph.getAllEdgesList( v1, v2 );

    if( matchers.size() == 1 && matchers.get( 0 ).acceptsAll() )
      return true;

    List<Scope> scopes = elementGraph.getAllEdgesList( v3, v4 );

    return areCompatibleEdges( plannerContext, elementGraph.getDelegate(), matchers, scopes ) != null;
    }

  public static Collection<Scope> areCompatibleEdges( PlannerContext plannerContext, ElementGraph elementGraph, List<ScopeExpression> matchers, List<Scope> scopes )
    {
    // if more than one matcher edge, ignore ANY/ALL path modifier
    if( matchers.size() == 1 )
      {
      ScopeExpression matcher = matchers.get( 0 );

      // test ALL
      if( matcher.appliesToAllPaths() )
        {
        for( Scope scope : scopes )
          {
          if( !matcher.applies( plannerContext, elementGraph, scope ) )
            return null;
          }

        return scopes;
        }

      if( matcher.appliesToAnyPath() )
        {
        for( Scope scope : scopes )
          {
          if( matcher.applies( plannerContext, elementGraph, scope ) )
            return Collections.singleton( scope );
          }

        return null;
        }

      if( matcher.appliesToEachPath() )
        {
        scopes = new LinkedList<>( scopes );

        ListIterator<Scope> iterator = scopes.listIterator();

        while( iterator.hasNext() )
          {
          if( !matcher.applies( plannerContext, elementGraph, iterator.next() ) )
            iterator.remove();
          }

        return scopes.isEmpty() ? null : scopes;
        }
      }

    // must have the same number of edges
    if( matchers.size() != scopes.size() )
      return null;

    // build a square matrix of all match permutations
    boolean[][] compat = new boolean[ matchers.size() ][ scopes.size() ];

    for( int i = 0; i < matchers.size(); i++ )
      {
      ScopeExpression matcher = matchers.get( i );

      for( int j = 0; j < scopes.size(); j++ )
        {
        Scope scope = scopes.get( j );

        compat[ i ][ j ] = matcher.applies( plannerContext, elementGraph, scope );
        }
      }

    // all matchers must fire for a given permutation
    List<Integer> range = new ArrayList<>();
    for( int i = 0; i < compat.length; i++ )
      range.add( i );

    PermutationIterator<Integer> iterator = new PermutationIterator<>( range );
    boolean[][] transformed = new boolean[ matchers.size() ][];

    while( iterator.hasNext() )
      {
      List<Integer> permutation = iterator.next();

      for( int i = 0; i < permutation.size(); i++ )
        transformed[ i ] = compat[ permutation.get( i ) ];

      boolean result = transformed[ 0 ][ 0 ];

      // test diagonal is true
      for( int i = 1; i < scopes.size(); i++ )
        result &= transformed[ i ][ i ];

      if( result )
        return scopes;
      }

    return null;
    }

  private boolean areCompatibleNodes( int node1, int node2 )
    {
    Expression expression = matchGraph.getVertex( node1 );
    FlowElement flowElement = elementGraph.getVertex( node2 );

    if(
      ( (ElementExpression) expression ).getCapture() == ElementExpression.Capture.Primary &&
        !finderContext.getRequiredElements().isEmpty()
      )
      return finderContext.isRequired( flowElement );

    if( finderContext.isExcluded( flowElement ) || finderContext.isIgnored( flowElement ) )
      return false;

    return expression.applies( plannerContext, elementGraph.getDelegate(), flowElement );
    }

  public boolean isFeasiblePair( int node1, int node2 )
    {
    assert node1 < n1;
    assert node2 < n2;
    assert core1[ node1 ] == NULL_NODE;
    assert core2[ node2 ] == NULL_NODE;

    if( !areCompatibleNodes( node1, node2 ) )
      return false;

    int termout1 = 0;
    int termout2 = 0;
    int termin1 = 0;
    int termin2 = 0;
    int new1 = 0;
    int new2 = 0;

    // Check the 'out' edges of node1
    for( int other1 : matchGraph.getSuccessors( node1 ) )
      {
      if( core1[ other1 ] != NULL_NODE )
        {
        int other2 = core1[ other1 ];
        // If there's node edge to the other node, or if there is some
        // edge incompatibility, then the mapping is not feasible
        if( !elementGraph.containsEdge( node2, other2 ) ||
          !areCompatibleEdges( node1, other1, node2, other2 ) )
          return false;
        }
      else
        {
        if( in1[ other1 ] != 0 )
          termin1++;
        if( out1[ other1 ] != 0 )
          termout1++;
        if( in1[ other1 ] == 0 && out1[ other1 ] == 0 )
          new1++;
        }
      }

    // Check the 'in' edges of node1
    for( int other1 : matchGraph.getPredecessors( node1 ) )
      {
      if( core1[ other1 ] != NULL_NODE )
        {
        int other2 = core1[ other1 ];
        // If there's node edge to the other node, or if there is some
        // edge incompatibility, then the mapping is not feasible
        if( !elementGraph.containsEdge( other2, node2 ) ||
          !areCompatibleEdges( other1, node1, other2, node2 ) )
          return false;
        }
      else
        {
        if( in1[ other1 ] != 0 )
          termin1++;
        if( out1[ other1 ] != 0 )
          termout1++;
        if( in1[ other1 ] == 0 && out1[ other1 ] == 0 )
          new1++;
        }
      }


    // Check the 'out' edges of node2
    for( int other2 : elementGraph.getSuccessors( node2 ) )
      {
      if( core2[ other2 ] != NULL_NODE )
        {
        int other1 = core2[ other2 ];
        if( !matchGraph.containsEdge( node1, other1 ) )
          return false;
        }
      else
        {
        if( in2[ other2 ] != 0 )
          termin2++;
        if( out2[ other2 ] != 0 )
          termout2++;
        if( in2[ other2 ] == 0 && out2[ other2 ] == 0 )
          new2++;
        }
      }

    // Check the 'in' edges of node2
    for( int other2 : elementGraph.getPredecessors( node2 ) )
      {
      if( core2[ other2 ] != NULL_NODE )
        {
        int other1 = core2[ other2 ];
        if( !matchGraph.containsEdge( other1, node1 ) )
          return false;
        }

      else
        {
        if( in2[ other2 ] != 0 )
          termin2++;
        if( out2[ other2 ] != 0 )
          termout2++;
        if( in2[ other2 ] == 0 && out2[ other2 ] == 0 )
          new2++;
        }
      }

    return termin1 <= termin2 && termout1 <= termout2 && new1 <= new2;
    }

  public void addPair( int node1, int node2 )
    {
    assert node1 < n1;
    assert node2 < n2;
    assert coreLen < n1;
    assert coreLen < n2;

    coreLen++;
    addedNode1 = node1;

    if( in1[ node1 ] == 0 )
      {
      in1[ node1 ] = coreLen;
      t1inLen++;

      if( out1[ node1 ] != 0 )
        t1bothLen++;
      }
    if( out1[ node1 ] == 0 )
      {
      out1[ node1 ] = coreLen;
      t1outLen++;

      if( in1[ node1 ] != 0 )
        t1bothLen++;
      }

    if( in2[ node2 ] == 0 )
      {
      in2[ node2 ] = coreLen;
      t2inLen++;

      if( out2[ node2 ] != 0 )
        t2bothLen++;
      }
    if( out2[ node2 ] == 0 )
      {
      out2[ node2 ] = coreLen;
      t2outLen++;

      if( in2[ node2 ] != 0 )
        t2bothLen++;
      }

    core1[ node1 ] = node2;
    core2[ node2 ] = node1;

    for( int other : matchGraph.getPredecessors( node1 ) )
      {
      if( in1[ other ] == 0 )
        {
        in1[ other ] = coreLen;
        t1inLen++;

        if( out1[ other ] != 0 )
          t1bothLen++;
        }
      }

    for( int other : matchGraph.getSuccessors( node1 ) )
      {
      if( out1[ other ] == 0 )
        {
        out1[ other ] = coreLen;
        t1outLen++;
        if( in1[ other ] != 0 )
          t1bothLen++;
        }
      }

    for( int other : elementGraph.getPredecessors( node2 ) )
      {
      if( in2[ other ] == 0 )
        {
        in2[ other ] = coreLen;
        t2inLen++;

        if( out2[ other ] != 0 )
          t2bothLen++;
        }
      }

    for( int other : elementGraph.getSuccessors( node2 ) )
      {
      if( out2[ other ] == 0 )
        {
        out2[ other ] = coreLen;
        t2outLen++;

        if( in2[ other ] != 0 )
          t2bothLen++;
        }
      }
    }

  public boolean isGoal()
    {
    return coreLen == n1;
    }

  public boolean isDead()
    {
    return n1 > n2
      || t1bothLen > t2bothLen
      || t1outLen > t2outLen
      || t1inLen > t2inLen;
    }

  public Map<Integer, Integer> getVertexMapping()
    {
    Map<Integer, Integer> vertexMapping = new HashMap<>();

    for( int i = 0; i < n1; ++i )
      {
      if( core1[ i ] != NULL_NODE )
        vertexMapping.put( i, core1[ i ] );
      }

    return vertexMapping;
    }

  public State copy()
    {
    return new State( this );
    }

  public void backTrack()
    {
    assert coreLen - origCoreLen <= 1;
    assert addedNode1 != NULL_NODE;

    if( origCoreLen >= coreLen )
      return;

    int node2;

    if( in1[ addedNode1 ] == coreLen )
      in1[ addedNode1 ] = 0;

    for( int other : matchGraph.getPredecessors( addedNode1 ) )
      {
      if( in1[ other ] == coreLen )
        in1[ other ] = 0;
      }

    if( out1[ addedNode1 ] == coreLen )
      out1[ addedNode1 ] = 0;

    for( int other : matchGraph.getSuccessors( addedNode1 ) )
      {
      if( out1[ other ] == coreLen )
        out1[ other ] = 0;
      }

    node2 = core1[ addedNode1 ];

    if( in2[ node2 ] == coreLen )
      in2[ node2 ] = 0;

    for( int other : elementGraph.getPredecessors( node2 ) )
      {
      if( in2[ other ] == coreLen )
        in2[ other ] = 0;
      }

    if( out2[ node2 ] == coreLen )
      out2[ node2 ] = 0;

    for( int other : elementGraph.getSuccessors( node2 ) )
      {
      if( out2[ other ] == coreLen )
        out2[ other ] = 0;
      }

    core1[ addedNode1 ] = NULL_NODE;
    core2[ node2 ] = NULL_NODE;

    coreLen = origCoreLen;
    addedNode1 = NULL_NODE;
    }

  public ElementExpression getMatcherNode( int vertex )
    {
    return matchGraph.getVertex( vertex );
    }

  public FlowElement getElementNode( int vertex )
    {
    return elementGraph.getVertex( vertex );
    }

  /**
   * Code below sourced from google guava in order to prevent version issues
   * <p/>
   * Copyright (C) 2007 The Guava Authors
   * Licensed under the Apache License, Version 2.0
   */
  private static class PermutationIterator<E> extends AbstractIterator<List<E>>
    {
    final List<E> list;
    final int[] c;
    final int[] o;
    int j;

    PermutationIterator( List<E> list )
      {
      this.list = new ArrayList<E>( list );
      int n = list.size();
      c = new int[ n ];
      o = new int[ n ];

      for( int i = 0; i < n; i++ )
        {
        c[ i ] = 0;
        o[ i ] = 1;
        }

      j = Integer.MAX_VALUE;
      }

    @Override
    protected List<E> computeNext()
      {
      if( j <= 0 )
        return endOfData();

      List<E> next = Collections.unmodifiableList( new ArrayList<>( list ) );
      calculateNextPermutation();

      return next;
      }

    void calculateNextPermutation()
      {
      j = list.size() - 1;
      int s = 0;

      // Handle the special case of an empty list. Skip the calculation of the
      // next permutation.
      if( j == -1 )
        return;

      while( true )
        {
        int q = c[ j ] + o[ j ];

        if( q < 0 )
          {
          switchDirection();
          continue;
          }

        if( q == j + 1 )
          {
          if( j == 0 )
            break;

          s++;
          switchDirection();
          continue;
          }

        Collections.swap( list, j - c[ j ] + s, j - q + s );
        c[ j ] = q;
        break;
        }
      }

    void switchDirection()
      {
      o[ j ] = -o[ j ];
      j--;
      }

    @Override
    public void remove()
      {
      throw new UnsupportedOperationException();
      }
    }

  private static abstract class AbstractIterator<T> implements Iterator<T>
    {
    private StateEnum state = StateEnum.NOT_READY;

    /** Constructor for use by subclasses. */
    protected AbstractIterator()
      {
      }

    private enum StateEnum
      {
        /** We have computed the next element and haven't returned it yet. */
        READY,

        /** We haven't yet computed or have already returned the element. */
        NOT_READY,

        /** We have reached the end of the data and are finished. */
        DONE,

        /** We've suffered an exception and are kaput. */
        FAILED,
      }

    private T next;

    protected abstract T computeNext();

    protected final T endOfData()
      {
      state = StateEnum.DONE;
      return null;
      }

    @Override
    public final boolean hasNext()
      {
      if( state == StateEnum.FAILED )
        throw new IllegalStateException();

      switch( state )
        {
        case DONE:
          return false;
        case READY:
          return true;
        default:
        }
      return tryToComputeNext();
      }

    private boolean tryToComputeNext()
      {
      state = StateEnum.FAILED; // temporary pessimism
      next = computeNext();
      if( state != StateEnum.DONE )
        {
        state = StateEnum.READY;
        return true;
        }
      return false;
      }

    @Override
    public final T next()
      {
      if( !hasNext() )
        throw new NoSuchElementException();

      state = StateEnum.NOT_READY;
      return next;
      }
    }
  }
