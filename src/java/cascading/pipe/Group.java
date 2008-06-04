/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.Scope;
import cascading.pipe.cogroup.CoGroupClosure;
import cascading.pipe.cogroup.CoGrouper;
import cascading.pipe.cogroup.GroupClosure;
import cascading.pipe.cogroup.InnerJoin;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TuplePair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;

/** The base class for {@link GroupBy} and {@link CoGroup}. */
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
  private int repeat = 1;
  /** Field coGrouper */
  private CoGrouper coGrouper;
  /** Field groupName */
  private String groupName;

  /** Field pipePos */
  private transient Map<String, Integer> pipePos;

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  public Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields );
    this.declaredFields = declaredFields;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    this.coGrouper = coGrouper;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, CoGrouper coGrouper )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields );
    this.coGrouper = coGrouper;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  public Group( Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    addPipe( lhs );
    addPipe( rhs );

    if( lhsGroupFields.size() != rhsGroupFields.size() )
      throw new IllegalArgumentException( "lhs and rhs cogroup fields must be same size" );

    groupFieldsMap.put( lhs.getName(), lhsGroupFields );
    groupFieldsMap.put( rhs.getName(), rhsGroupFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes of type Pipe...
   */
  public Group( Pipe... pipes )
    {
    this( pipes, null );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  public Group( Pipe[] pipes, Fields[] groupFields )
    {
    int last = -1;
    for( int i = 0; i < pipes.length; i++ )
      {
      addPipe( pipes[ i ] );

      if( groupFields == null || groupFields.length == 0 )
        {
        groupFieldsMap.put( pipes[ i ].getName(), Fields.FIRST );
        continue;
        }

      if( last != -1 && last != groupFields[ i ].size() )
        throw new IllegalArgumentException( "all cogroup fields must be same size" );

      last = groupFields[ i ].size();
      groupFieldsMap.put( pipes[ i ].getName(), groupFields[ i ] );
      }
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   */
  public Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, Fields declaredFields, CoGrouper coGrouper )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, declaredFields, coGrouper );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields, CoGrouper coGrouper )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields, coGrouper );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param lhs            of type Pipe
   * @param lhsGroupFields of type Fields
   * @param rhs            of type Pipe
   * @param rhsGroupFields of type Fields
   */
  public Group( String groupName, Pipe lhs, Fields lhsGroupFields, Pipe rhs, Fields rhsGroupFields )
    {
    this( lhs, lhsGroupFields, rhs, rhsGroupFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param pipes     of type Pipe...
   */
  public Group( String groupName, Pipe... pipes )
    {
    this( pipes );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName
   * @param pipes       of type Pipe[]
   * @param groupFields of type Fields[]
   */
  public Group( String groupName, Pipe[] pipes, Fields[] groupFields )
    {
    this( pipes, groupFields );
    this.groupName = groupName;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param repeat         of type int
   * @param declaredFields of type Fields
   */
  public Group( Pipe pipe, Fields groupFields, int repeat, Fields declaredFields )
    {
    this( pipe, groupFields, repeat );
    this.declaredFields = declaredFields;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe           of type Pipe
   * @param groupFields    of type Fields
   * @param repeat         of type int
   * @param declaredFields of type Fields
   * @param coGrouper      of type CoGrouper
   */
  public Group( Pipe pipe, Fields groupFields, int repeat, Fields declaredFields, CoGrouper coGrouper )
    {
    this( pipe, groupFields, repeat, declaredFields );
    this.coGrouper = coGrouper;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param repeat      of type int
   * @param coGrouper   of type CoGrouper
   */
  public Group( Pipe pipe, Fields groupFields, int repeat, CoGrouper coGrouper )
    {
    this( pipe, groupFields, repeat );
    this.coGrouper = coGrouper;
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param repeat      of type int
   */
  public Group( Pipe pipe, Fields groupFields, int repeat )
    {
    addPipe( pipe );
    this.groupFieldsMap.put( pipe.getName(), groupFields );
    this.repeat = repeat;
    }

  /**
   * Constructor Group creates a new Group instance where grouping occurs on {@link Fields#ALL} fields.
   *
   * @param pipe of type Pipe
   */
  public Group( Pipe pipe )
    {
    addPipe( pipe );
    this.groupFieldsMap.put( pipe.getName(), Fields.ALL );
    this.coGrouper = new InnerJoin();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public Group( Pipe pipe, Fields groupFields )
    {
    addPipe( pipe );
    this.groupFieldsMap.put( pipe.getName(), groupFields );
    this.coGrouper = new InnerJoin();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   */
  public Group( String groupName, Pipe pipe, Fields groupFields )
    {
    addPipe( pipe );
    this.groupName = groupName;
    this.groupFieldsMap.put( pipe.getName(), groupFields );
    this.coGrouper = new InnerJoin();
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public Group( Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( pipe, groupFields );
    this.sortFieldsMap.put( pipe.getName(), sortFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param groupName   of type String
   * @param pipe        of type Pipe
   * @param groupFields of type Fields
   * @param sortFields  of type Fields
   */
  public Group( String groupName, Pipe pipe, Fields groupFields, Fields sortFields )
    {
    this( groupName, pipe, groupFields );
    addPipe( pipe );
    this.sortFieldsMap.put( pipe.getName(), sortFields );
    }

  /**
   * Constructor Group creates a new Group instance.
   *
   * @param pipe         of type Pipe
   * @param groupFields  of type Fields
   * @param sortFields   of type Fields
   * @param reverseOrder of type boolean
   */
  public Group( Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( pipe, groupFields );
    this.reverseOrder = reverseOrder;
    this.sortFieldsMap.put( pipe.getName(), sortFields );
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
  public Group( String groupName, Pipe pipe, Fields groupFields, Fields sortFields, boolean reverseOrder )
    {
    this( groupName, pipe, groupFields );
    this.reverseOrder = reverseOrder;
    this.sortFieldsMap.put( pipe.getName(), sortFields );
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
      throw new IllegalArgumentException( "pipe must have name" );

    pipes.add( pipe ); // allow same pipe
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
        buffer.append( "+" );

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
   * @param output        of type OutputCollector
   */
  public void collectReduceGrouping( Scope incomingScope, Scope outgoingScope, TupleEntry entry, OutputCollector output ) throws IOException
    {
    Fields groupFields = outgoingScope.getGroupingSelectors().get( incomingScope.getName() );
    Fields sortFields = outgoingScope.getSortingSelectors() == null ? null : outgoingScope.getSortingSelectors().get( incomingScope.getName() );

    if( LOG.isDebugEnabled() )
      LOG.debug( "cogroup: [" + incomingScope + "] key pos: [" + groupFields + "]" );

    // todo: would be nice to delegate this back to the GroupClosure
    Tuple groupTuple = entry.extractTuple( groupFields ); // we are nulling dupe values here to reduce bandwidth usage
    Tuple sortTuple = sortFields == null ? null : entry.selectTuple( sortFields );
    Tuple valuesTuple = entry.getTuple();

    WritableComparable groupKey = sortTuple == null ? groupTuple : new TuplePair( groupTuple, sortTuple );

    if( isGroupBy() )
      output.collect( groupKey, valuesTuple );
    else
      output.collect( groupKey, new Tuple( getPipePos().get( incomingScope.getName() ), valuesTuple ) );
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
   * @param jobConf       of type JobConf
   * @param outgoingScope of type Scope
   * @param key           of type WritableComparable
   * @param values        of type Iterator @return Iterator<Tuple>
   */
  public Iterator<Tuple> iterateReduceValues( JobConf jobConf, Set<Scope> incomingScopes, Scope outgoingScope, WritableComparable key, Iterator values )
    {
    GroupClosure closure;

    if( isGroupBy() )
      {
      Scope incomingScope = incomingScopes.iterator().next();
      Fields[] groupFields = Fields.fields( outgoingScope.getGroupingSelectors().get( incomingScope.getName() ) );
      Fields[] valuesFields = Fields.fields( incomingScope.getOutValuesFields() );

      closure = new GroupClosure( groupFields, valuesFields, (Tuple) key, values );
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

      closure = new CoGroupClosure( jobConf, repeat, groupFields, valuesFields, (Tuple) key, values );
      }

    if( coGrouper == null )
      return new InnerJoin().getIterator( closure );
    else
      return coGrouper.getIterator( closure );
    }

  private boolean isGroupBy()
    {
    return Math.max( pipes.size(), repeat ) == 1;
    }

  // FIELDS

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Map<String, Fields> groupingSelectors = resolveGroupingSelectors( incomingScopes );
    Map<String, Fields> sortingSelectors = resolveSortingSelectors( incomingScopes );
    Fields declared = resolveDeclared( incomingScopes );

    // for Group, the outgoing fields are the same as those declared
    return new Scope( getName(), declared, groupingSelectors, sortingSelectors, declared );
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
          throw new OperatorException( "all grouping fields must be same size:" + toString() );

        size = groupingField.size();
        }

      return groupingFields;
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( "could not resolve grouping selector in: " + this, exception );
      }
    }

  private Map<String, Fields> resolveSelectorsAgainstIncoming( Set<Scope> incomingScopes, Map<String, Fields> selectors, String type )
    {
    Map<String, Fields> resolvedFields = new HashMap<String, Fields>();

    for( Scope incomingScope : incomingScopes )
      {
      Fields selector = selectors.get( incomingScope.getName() );

      if( selector == null )
        throw new OperatorException( "no " + type + " selector found for: " + incomingScope.getName() );

      Fields incomingFields;

      if( selector.isAll() )
        incomingFields = resolveFields( incomingScope );
      else if( selector.isKeys() )
        incomingFields = incomingScope.getOutGroupingFields();
      else if( selector.isValues() )
        incomingFields = incomingScope.getOutValuesFields().minus( incomingScope.getOutGroupingFields() );
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
    catch( RuntimeException exception )
      {
      throw new OperatorException( "could not resolve sorting selector in: " + this, exception );
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

      if( declaredFields != null )
        {
        int size = 0;

        for( Scope incomingScope : incomingScopes )
          size += resolveFields( incomingScope ).size();

        if( declaredFields.size() != size * repeat )
          throw new OperatorException( "declared grouped fields not same size as grouped values, declared: " + declaredFields.size() + " != size: " + size * repeat );

        return declaredFields;
        }

      Fields appendedFields = new Fields();

      // will fail on name collisions
      for( Scope incomingScope : incomingScopes )
        appendedFields = appendedFields.append( resolveFields( incomingScope ) );

      return appendedFields;
      }
    catch( RuntimeException exception )
      {
      throw new OperatorException( "could not resolve declared fields in: " + this, exception );
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

      buffer.append( groupFieldsMap.get( name ).print() );
      }

    if( repeat != 1 )
      buffer.append( "[repeat:" ).append( repeat ).append( "]" );

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

      if( repeat != 1 )
        buffer.append( "[repeat:" ).append( repeat ).append( "]" );
      }

    buffer.append( "]" );
    }
  }
