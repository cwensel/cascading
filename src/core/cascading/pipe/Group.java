/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.cogroup.CoGroupClosure;
import cascading.pipe.cogroup.GroupClosure;
import cascading.pipe.cogroup.InnerJoin;
import cascading.pipe.cogroup.Joiner;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.IndexTuple;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleException;
import cascading.tuple.TuplePair;
import cascading.tuple.Tuples;
import cascading.util.Util;
import org.apache.log4j.Logger;

/**
 * The base class for {@link GroupBy} and {@link CoGroup}. This class should not be used directly.
 *
 * @see GroupBy
 * @see CoGroup
 */
public class Group extends Pipe
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( Group.class );

  /** Field pipes */
  private final List<Pipe> pipes = new ArrayList<Pipe>();
  /** Field groupFieldsMap */
  protected final Map<String, Fields> groupFieldsMap = new LinkedHashMap<String, Fields>(); // keep order
  /** Field sortFieldsMap */
  protected Map<String, Fields> sortFieldsMap = new LinkedHashMap<String, Fields>(); // keep order
  /** Field reverseOrder */
  private boolean reverseOrder = false;
  /** Field declaredFields */
  protected Fields declaredFields;
  /** Field repeat */
  private int numSelfJoins = 0;
  /** Field coGrouper */
  private Joiner joiner;
  /** Field groupName */
  private String groupName;
  /** Field isGroupBy */
  private boolean isGroupBy;

  /** Field pipePos */
  private transient Map<String, Integer> pipePos;
  private GroupClosure closure;

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  protected Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    this( Pipe.pipes( lhs, rhs ), Fields.fields( lhsGroupFields, rhsGroupFields ), declaredFields, joiner );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, null, joiner );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  protected Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    this( Pipe.pipes( lhs, rhs ), Fields.fields( lhsGroupFields, rhsGroupFields ) );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes of type Pipe...
   */
  protected Group( Pipe... pipes )
    {
    this( pipes, (Fields[]) null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  protected Group( Pipe[] pipes, Fields[] groupFields )
    {
    this( null, pipes, groupFields, null, null );
    }


  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  protected Group( String groupName, Pipe[] pipes, Fields[] groupFields )
    {
    this( groupName, pipes, groupFields, null, null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   */
  protected Group( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields )
    {
    this( groupName, pipes, groupFields, declaredFields, null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    this( null, pipes, groupFields, declaredFields, joiner );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param pipes          of type Pipe[]
   * @param groupFields    of type Fields[]
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( String groupName, Pipe[] pipes, Fields[] groupFields, Fields declaredFields, Joiner joiner )
    {
    this.groupName = groupName;

    int last = -1;
    for( int i = 0; i < pipes.length; i++ )
      {
      addPipe( pipes[ i ] );

      if( groupFields == null || groupFields.length == 0 )
        {
        addGroupFields( pipes[ i ], Fields.FIRST );
        continue;
        }

      if( last != -1 && last != groupFields[ i ].size() )
        throw new IllegalArgumentException( "all cogroup fields must be same size" );

      last = groupFields[ i ].size();
      addGroupFields( pipes[ i ], groupFields[ i ] );
      }

    this.declaredFields = declaredFields;
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  protected Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, joiner );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Joiner joiner )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, joiner );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  protected Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName of type String
   * @param pipes     of type Pipe...
   */
  protected Group( String groupName, Pipe... pipes )
    {
    this( pipes );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  protected Group( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    this( pipe, groupFields, numSelfJoins );
    this.declaredFields = declaredFields;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields );
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  protected Group( Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    addPipe( pipe );
    this.groupFieldsMap.put( pipe.getName(), groupFields );
    this.numSelfJoins = numSelfJoins;
    this.joiner = joiner;

    verifyCoGrouper();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  protected Group( Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    this( pipe, groupFields, numSelfJoins, (Joiner) null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName      of type String
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param numSelfJoins   of type int
   * @param declaredFields of type Fields
   * @param joiner         of type CoGrouper
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Fields declaredFields, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, declaredFields, joiner );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   * @param joiner       of type CoGrouper
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins, Joiner joiner )
    {
    this( pipe, groupFields, numSelfJoins, joiner );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param numSelfJoins of type int
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, int numSelfJoins )
    {
    this( pipe, groupFields, numSelfJoins );
    this.groupName = groupName;
    }

  ////////////
  // GROUPBY
  ////////////

  /**
   * Constructor Group creates a new Group instance where grouping occurs on {@link Fields#ALL} fields.
   *
   * @param pipe of type Pipe
   */
  protected Group( Pipe pipe )
    {
    this( null, pipe, Fields.ALL, null, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  protected Group( Pipe pipe, Fields groupFields )
    {
    this( null, pipe, groupFields, null, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields )
    {
    this( groupName, pipe, groupFields, null, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Group( Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( null, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( groupName, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Group( Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( null, pipe, groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName    of type String
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Group( String groupName, Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( groupName, Pipe.pipes( pipe ), groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  protected Group( Pipe[] pipes, Fields groupFields )
    {
    this( null, pipes, groupFields, null, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   */
  protected Group( String groupName, Pipe[] pipes, Fields groupFields )
    {
    this( groupName, pipes, groupFields, null, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes       of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Group( Pipe[] pipes, Fields groupFields, Fields sortFields )
    {
    this( null, pipes, groupFields, sortFields, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  protected Group( String groupName, Pipe[] pipe, Fields groupFields, Fields sortFields )
    {
    this( groupName, pipe, groupFields, sortFields, false );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes        of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Group( Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( null, pipes, groupFields, sortFields, reverseOrder );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName    of type String
   * @param pipes        of type Pipe[]
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  protected Group( String groupName, Pipe[] pipes, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this.isGroupBy = true;
    this.groupName = groupName;

    for( Pipe pipe : pipes )
      {
      addPipe( pipe );
      this.groupFieldsMap.put( pipe.getName(), groupFields );

      if( sortFields != null )
        this.sortFieldsMap.put( pipe.getName(), sortFields );
      }

    this.reverseOrder = reverseOrder;
    this.joiner = new InnerJoin();
    }

  private void verifyCoGrouper()
    {
    if( joiner == null )
      {
      joiner = new InnerJoin();
      return;
      }

    if( joiner.numJoins() == -1 )
      return;

    int joins = Math.max( numSelfJoins, groupFieldsMap.size() - 1 ); // joining two streams is one join

    if( joins != joiner.numJoins() )
      throw new IllegalArgumentException( "invalid cogrouper, only accepts " + joiner.numJoins() + " joins, there are: " + joins );
    }

  /**
   * Method getDeclaredFields returns the declaredFields of this Group object.
   *
   * @return the declaredFields (type Fields) of this Group object.
   */
  public Fields getDeclaredFields()
    {
    return declaredFields;
    }

  private void addPipe( Pipe pipe )
    {
    if( pipe.getName() == null )
      throw new IllegalArgumentException( "each input pipe must have a name" );

    pipes.add( pipe ); // allow same pipe
    }

  private void addGroupFields( Pipe pipe, Fields fields )
    {
    if( groupFieldsMap.containsKey( pipe.getName() ) )
      throw new IllegalArgumentException( "each input pipe branch must be uniquely named" );

    groupFieldsMap.put( pipe.getName(), fields );
    }

  @Override
  public String getName()
    {
    if( groupName != null )
      return groupName;

    StringBuffer buffer = new StringBuffer();

    for( Pipe pipe : pipes )
      {
      if( buffer.length() != 0 )
        {
        if( isGroupBy )
          buffer.append( "+" );
        else
          buffer.append( "*" ); // more semantically correct
        }

      buffer.append( pipe.getName() );
      }

    groupName = buffer.toString();

    return groupName;
    }

  @Override
  public Pipe[] getPrevious()
    {
    return pipes.toArray( new Pipe[pipes.size()] );
    }

  /**
   * Method getGroupingSelectors returns the groupingSelectors of this Group object.
   *
   * @return the groupingSelectors (type Map<String, Fields>) of this Group object.
   */
  public Map<String, Fields> getGroupingSelectors()
    {
    return groupFieldsMap;
    }

  /**
   * Method getSortingSelectors returns the sortingSelectors of this Group object.
   *
   * @return the sortingSelectors (type Map<String, Fields>) of this Group object.
   */
  public Map<String, Fields> getSortingSelectors()
    {
    return sortFieldsMap;
    }

  /**
   * Method isSorted returns true if this Group instance is sorting values other than the group fields.
   *
   * @return the sorted (type boolean) of this Group object.
   */
  public boolean isSorted()
    {
    return !sortFieldsMap.isEmpty();
    }

  /**
   * Method isSortReversed returns true if sorting is reversed.
   *
   * @return the sortReversed (type boolean) of this Group object.
   */
  public boolean isSortReversed()
    {
    return reverseOrder;
    }

  private Map<String, Integer> getPipePos()
    {
    if( pipePos != null )
      return pipePos;

    pipePos = new HashMap<String, Integer>();

    int pos = 0;
    for( Object pipe : pipes )
      pipePos.put( ( (Pipe) pipe ).getName(), pos++ );

    return pipePos;
    }

  /**
   * Method makeReduceGrouping makes a group Tuple[] of the form [ ['grpValue', ...] [ sourceName, [ 'value', ...] ] ]
   * <p/>
   * Since this is a join, we must track from which source a given tuple is sourced from so we can
   * cogroup properly at the reduce stage.
   *
   * @param incomingScope of type Scope
   * @param outgoingScope of type Scope
   * @param entry         of type TupleEntry
   * @param flowProcess   of type OutputCollector
   * @throws IOException thrown by OutputCollector on collect
   */
  public void collectReduceGrouping( Scope incomingScope, Scope outgoingScope, TupleEntry entry, HadoopFlowProcess flowProcess ) throws IOException, InterruptedException
    {
    Fields groupFields = outgoingScope.getGroupingSelectors().get( incomingScope.getName() );
    Fields sortFields = outgoingScope.getSortingSelectors() == null ? null : outgoingScope.getSortingSelectors().get( incomingScope.getName() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "cogroup: [" + incomingScope + "] key pos: [" + groupFields + "]" );

    // todo: would be nice to delegate this back to the GroupClosure
    Tuple groupTuple = Tuples.extractTuple( entry, groupFields ); // we are nulling dupe values here to reduce bandwidth usage
    Tuple sortTuple = sortFields == null ? null : entry.selectTuple( sortFields );
    Tuple valuesTuple = entry.getTuple();

    Tuple groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    if( isGroupBy() )
      flowProcess.getContext().write( groupKey, valuesTuple );
    else
      flowProcess.getContext().write( groupKey, new IndexTuple( getPipePos().get( incomingScope.getName() ), valuesTuple ) );
    }

  /**
   * Method unwrapGrouping tests if the given grouping key Tuple should be unwrapped if this Group instance is sorting.
   *
   * @param tuple of type Tuple
   * @return Tuple
   */
  public Tuple unwrapGrouping( Tuple tuple )
    {
    return !isSorted() ? (Tuple) tuple : ( (TuplePair) tuple ).getLhs();
    }

  /**
   * Method makeReduceValues wrapps the incoming Hadoop value stream as an iterator over {@link Tuple} instance.
   *
   * @param key    of type WritableComparable
   * @param values of type Iterator @return Iterator<Tuple>
   * @return a Tuple Iterator
   */
  public Iterator<Tuple> iterateReduceValues( Tuple key, Iterator values )
    {
    closure.reset( key, values );

    return joiner.getIterator( closure );
    }

  public void initializeReduce( FlowProcess flowProcess, Set<Scope> incomingScopes, Scope outgoingScope )
    {
    if( isGroupBy() )
      {
      Scope incomingScope = incomingScopes.iterator().next();
      Fields[] groupFields = Fields.fields( outgoingScope.getGroupingSelectors().get( incomingScope.getName() ) );
      Fields[] valuesFields = Fields.fields( incomingScope.getOutValuesFields() );

      closure = new GroupClosure( groupFields, valuesFields );
      }
    else
      {
      Fields[] groupFields = new Fields[pipes.size()];
      Fields[] valuesFields = new Fields[pipes.size()];

      for( Scope incomingScope : incomingScopes )
        {
        int pos = getPipePos().get( incomingScope.getName() );

        groupFields[ pos ] = outgoingScope.getGroupingSelectors().get( incomingScope.getName() );
        valuesFields[ pos ] = incomingScope.getOutValuesFields();
        }

      closure = new CoGroupClosure( flowProcess, numSelfJoins, groupFields, valuesFields );
      }
    }

  /**
   * Method isGroupBy returns true if this Group instance will perform a GroupBy operation.
   *
   * @return the groupBy (type boolean) of this Group object.
   */
  public boolean isGroupBy()
    {
    return isGroupBy;
    }

  boolean isSelfJoin()
    {
    return numSelfJoins != 0;
    }

  // FIELDS

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Map<String, Fields> groupingSelectors = resolveGroupingSelectors( incomingScopes );
    Map<String, Fields> sortingSelectors = resolveSortingSelectors( incomingScopes );
    Fields declared = resolveDeclared( incomingScopes );

    // for Group, the outgoing fields are the same as those declared
    return new Scope( getName(), declared, groupingSelectors, sortingSelectors, declared, isGroupBy() );
    }

  Map<String, Fields> resolveGroupingSelectors( Set<Scope> incomingScopes )
    {
    try
      {
      Map<String, Fields> groupingSelectors = getGroupingSelectors();
      Map<String, Fields> groupingFields = resolveSelectorsAgainstIncoming( incomingScopes, groupingSelectors, "grouping" );

      Iterator<Fields> iterator = groupingFields.values().iterator();
      int size = iterator.next().size();

      while( iterator.hasNext() )
        {
        Fields groupingField = iterator.next();

        if( groupingField.size() != size )
          throw new OperatorException( this, "all grouping fields must be same size:" + toString() );

        size = groupingField.size();
        }

      return groupingFields;
      }
    catch( FieldsResolverException exception )
      {
      throw new OperatorException( this, OperatorException.Kind.grouping, exception.getSourceFields(), exception.getSelectorFields(), exception );
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve grouping selector in: " + this, exception );
      }
    }

  private Map<String, Fields> resolveSelectorsAgainstIncoming( Set<Scope> incomingScopes, Map<String, Fields> selectors, String type )
    {
    Map<String, Fields> resolvedFields = new HashMap<String, Fields>();

    for( Scope incomingScope : incomingScopes )
      {
      Fields selector = selectors.get( incomingScope.getName() );

      if( selector == null )
        throw new OperatorException( this, "no " + type + " selector found for: " + incomingScope.getName() );

      Fields incomingFields;

      if( selector.isAll() )
        incomingFields = resolveFields( incomingScope );
      else if( selector.isGroup() )
        incomingFields = incomingScope.getOutGroupingFields();
      else if( selector.isValues() )
          incomingFields = incomingScope.getOutValuesFields().subtract( incomingScope.getOutGroupingFields() );
        else
          incomingFields = resolveFields( incomingScope ).select( selector );

      resolvedFields.put( incomingScope.getName(), incomingFields );
      }

    return resolvedFields;
    }

  Map<String, Fields> resolveSortingSelectors( Set<Scope> incomingScopes )
    {
    try
      {
      if( getSortingSelectors().isEmpty() )
        return null;

      return resolveSelectorsAgainstIncoming( incomingScopes, getSortingSelectors(), "sorting" );
      }
    catch( FieldsResolverException exception )
      {
      throw new OperatorException( this, OperatorException.Kind.sorting, exception.getSourceFields(), exception.getSelectorFields(), exception );
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve sorting selector in: " + this, exception );
      }
    }

  @Override
  public Fields resolveFields( Scope scope )
    {
    if( scope.isEvery() )
      return scope.getOutGroupingFields();
    else
      return scope.getOutValuesFields();
    }

  Fields resolveDeclared( Set<Scope> incomingScopes )
    {
    try
      {
      Fields declaredFields = getDeclaredFields();

      if( declaredFields != null ) // null for GroupBy
        {
        if( incomingScopes.size() != pipes.size() && isSelfJoin() )
          throw new OperatorException( this, "self joins without intermediate operators are not permitted, see 'numSelfJoins' constructor or identity function" );

        int size = 0;
        boolean foundUnknown = false;

        List<Fields> resolvedFields = new ArrayList<Fields>();

        for( Scope incomingScope : incomingScopes )
          {
          Fields fields = resolveFields( incomingScope );

          foundUnknown = foundUnknown || fields.isUnknown();
          size += fields.size();

          resolvedFields.add( fields );
          }

        // we must relax field checking in the face of unkown fields
        if( !foundUnknown && declaredFields.size() != size * ( numSelfJoins + 1 ) )
          {
          if( isSelfJoin() )
            throw new OperatorException( this, "declared grouped fields not same size as grouped values, declared: " + declaredFields.printVerbose() + " != size: " + size * ( numSelfJoins + 1 ) );
          else
            throw new OperatorException( this, "declared grouped fields not same size as grouped values, declared: " + declaredFields.printVerbose() + " resolved: " + Util.print( resolvedFields, "" ) );
          }

        return declaredFields;
        }

      // support merge or cogrouping here
      if( isGroupBy() )
        {
        Fields commonFields = null;

        for( Scope incomingScope : incomingScopes )
          {
          Fields fields = resolveFields( incomingScope );

          if( commonFields == null )
            commonFields = fields;
          else if( !commonFields.equals( fields ) )
            throw new OperatorException( this, "merged streams must declare the same field names, expected: " + commonFields.printVerbose() + " found: " + fields.print() );
          }

        return commonFields;
        }
      else
        {
        Map<String, Scope> scopesMap = new HashMap<String, Scope>();

        for( Scope incomingScope : incomingScopes )
          scopesMap.put( incomingScope.getName(), incomingScope );

        List<Fields> appendableFields = new ArrayList<Fields>();

        for( Pipe pipe : pipes )
          appendableFields.add( resolveFields( scopesMap.get( pipe.getName() ) ) );

        Fields appendedFields = new Fields();

        try
          {
          // will throwFail on name collisions
          for( Fields appendableField : appendableFields )
            appendedFields = appendedFields.append( appendableField );
          }
        catch( TupleException exception )
          {
          String fields = "";

          for( Fields appendableField : appendableFields )
            fields += appendableField.print();

          throw new OperatorException( this, "found duplicate field names in cogrouped tuple stream: " + fields, exception );
          }

        return appendedFields;
        }
      }
    catch( OperatorException exception )
      {
      throw exception;
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( this, "could not resolve declared fields in: " + this, exception );
      }
    }

  Fields resolveOutgoingSelector( Fields declared )
    {
    return declared;
    }

  // OBJECT OVERRIDES

  @Override
  @SuppressWarnings({"RedundantIfStatement"})
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    Group group = (Group) object;

    if( groupName != null ? !groupName.equals( group.groupName ) : group.groupName != null )
      return false;
    if( groupFieldsMap != null ? !groupFieldsMap.equals( group.groupFieldsMap ) : group.groupFieldsMap != null )
      return false;
    if( pipes != null ? !pipes.equals( group.pipes ) : group.pipes != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( pipes != null ? pipes.hashCode() : 0 );
    result = 31 * result + ( groupFieldsMap != null ? groupFieldsMap.hashCode() : 0 );
    result = 31 * result + ( groupName != null ? groupName.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    StringBuilder buffer = new StringBuilder( super.toString() );

    buffer.append( "[by:" );

    for( String name : groupFieldsMap.keySet() )
      {
      if( groupFieldsMap.size() > 1 )
        buffer.append( name ).append( ":" );

      buffer.append( groupFieldsMap.get( name ).printVerbose() );
      }

    if( isSelfJoin() )
      buffer.append( "[numSelfJoins:" ).append( numSelfJoins ).append( "]" );

    buffer.append( "]" );

    return buffer.toString();
    }

  @Override
  protected void printInternal( StringBuffer buffer, Scope scope )
    {
    super.printInternal( buffer, scope );
    buffer.append( "[by:" );

    Map<String, Fields> map = scope.getGroupingSelectors();

    if( map != null )
      {
      for( String name : map.keySet() )
        {
        if( map.size() > 1 )
          buffer.append( name ).append( ":" );

        buffer.append( map.get( name ).print() );
        }

      if( isSelfJoin() )
        buffer.append( "[numSelfJoins:" ).append( numSelfJoins ).append( "]" );
      }

    buffer.append( "]" );
    }
  }
