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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.FlowProps;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.pipe.Splice;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.Tuples;
import cascading.tuple.util.TupleBuilder;
import cascading.tuple.util.TupleHasher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.tuple.util.TupleViews.*;

/**
 *
 */
public abstract class SpliceGate extends Gate<TupleEntry, Grouping<TupleEntry, TupleEntryIterator>> implements ElementDuct, Collapsing
  {
  private static final Logger LOG = LoggerFactory.getLogger( SpliceGate.class );

  protected Map<Duct, Integer> ordinalMap;

  public enum Role
    {
      sink, source, both
    }

  protected final FlowProcess flowProcess;
  protected Role role = Role.both;

  private TrapHandler trapHandler;
  private Set<String> branchNames;

  protected final Splice splice;
  protected final List<Scope> incomingScopes = new ArrayList<Scope>();
  protected final List<Scope> outgoingScopes = new ArrayList<Scope>();
  protected Fields[] keyFields;
  protected Fields[] sortFields;
  protected Fields[] valuesFields;

  protected Comparator<Tuple>[] groupComparators;
  protected Comparator<Tuple>[] valueComparators;
  protected TupleHasher groupHasher;
  protected boolean nullsAreNotEqual;

  protected TupleBuilder[] keyBuilder;
  protected TupleBuilder[] valuesBuilder;
  protected TupleBuilder[] sortBuilder;

  protected Grouping<TupleEntry, TupleEntryIterator> grouping;
  protected TupleEntry keyEntry;
  protected TupleEntryChainIterator tupleEntryIterator;

  public SpliceGate( FlowProcess flowProcess, Splice splice )
    {
    this.splice = splice;

    FlowElement element = splice;

    while( element != null )
      {
      if( element.hasConfigDef() )
        flowProcess = new ElementFlowProcess( flowProcess, element.getConfigDef() );

      element = ( (Pipe) element ).getParent();
      }

    this.flowProcess = flowProcess;
    }

  public SpliceGate( FlowProcess flowProcess, Splice splice, Role role )
    {
    this.splice = splice;
    this.flowProcess = flowProcess;
    this.role = role;
    }

  @Override
  public void bind( StreamGraph streamGraph )
    {
    super.bind( streamGraph );

    setOrdinalMap( streamGraph );
    }

  protected synchronized void setOrdinalMap( StreamGraph streamGraph )
    {
    ordinalMap = streamGraph.getOrdinalMap( this );
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

  protected TupleBuilder createNarrowBuilder( final Fields incomingFields, final Fields narrowFields )
    {
    if( narrowFields.isNone() )
      return new TupleBuilder()
      {
      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return Tuple.NULL;
        }
      };

    if( incomingFields.isUnknown() )
      return new TupleBuilder()
      {
      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return input.get( incomingFields, narrowFields );
        }
      };

    if( narrowFields.isAll() ) // dubious this is ever reached
      return new TupleBuilder()
      {
      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return input;
        }
      };

    return createDefaultNarrowBuilder( incomingFields, narrowFields );
    }

  protected TupleBuilder createDefaultNarrowBuilder( final Fields incomingFields, final Fields narrowFields )
    {
    return new TupleBuilder()
    {
    Tuple result = createNarrow( incomingFields.getPos( narrowFields ) );

    @Override
    public Tuple makeResult( Tuple input, Tuple output )
      {
      return reset( result, input );
      }
    };
    }

  protected TupleBuilder createNulledBuilder( final Fields incomingFields, final Fields keyField )
    {
    if( incomingFields.isUnknown() )
      return new TupleBuilder()
      {
      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return Tuples.nulledCopy( incomingFields, input, keyField );
        }
      };

    if( keyField.isNone() )
      return new TupleBuilder()
      {
      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return input;
        }
      };

    if( keyField.isAll() )
      return new TupleBuilder()
      {
      Tuple nullTuple = Tuple.size( incomingFields.size() );

      @Override
      public Tuple makeResult( Tuple input, Tuple output )
        {
        return nullTuple;
        }
      };

    return new TupleBuilder()
    {
    Tuple nullTuple = Tuple.size( keyField.size() );
    Tuple result = createOverride( incomingFields, keyField );

    @Override
    public Tuple makeResult( Tuple baseTuple, Tuple output )
      {
      return reset( result, baseTuple, nullTuple );
      }
    };
    }

  @Override
  public void initialize()
    {
    super.initialize();

    if( incomingScopes.size() == 0 )
      throw new IllegalStateException( "incoming scopes may not be empty" );

    if( outgoingScopes.size() == 0 )
      throw new IllegalStateException( "outgoing scope may not be empty" );

    int size = getNumDeclaredIncomingBranches(); // is the maximum ordinal value

    // this is a merge, all fields have the same declaration
    // filling out full array has implications on joiner/closure which should be resolved independently
    if( role == Role.source && splice.isGroupBy() )
      size = 1;

    keyFields = new Fields[ size ];
    valuesFields = new Fields[ size ];

    keyBuilder = new TupleBuilder[ size ];
    valuesBuilder = new TupleBuilder[ size ];

    if( splice.isSorted() )
      {
      sortFields = new Fields[ size ];
      sortBuilder = new TupleBuilder[ size ];
      }

    Scope outgoingScope = outgoingScopes.get( 0 );

    int numScopes = Math.min( size, incomingScopes.size() );
    for( int i = 0; i < numScopes; i++ )
      {
      Scope incomingScope = incomingScopes.get( i );

      // for GroupBy, incoming may have same name, but guaranteed to have same key/value/sort fields for merge
      // arrays may be size 1, then ordinal should always be zero.
      int ordinal = size == 1 ? 0 : incomingScope.getOrdinal();

      keyFields[ ordinal ] = outgoingScope.getKeySelectors().get( incomingScope.getName() );
      valuesFields[ ordinal ] = incomingScope.getIncomingSpliceFields();

      keyBuilder[ ordinal ] = createNarrowBuilder( incomingScope.getIncomingSpliceFields(), keyFields[ ordinal ] );
      valuesBuilder[ ordinal ] = createNulledBuilder( incomingScope.getIncomingSpliceFields(), keyFields[ ordinal ] );

      if( sortFields != null )
        {
        sortFields[ ordinal ] = outgoingScope.getSortingSelectors().get( incomingScope.getName() );
        sortBuilder[ ordinal ] = createNarrowBuilder( incomingScope.getIncomingSpliceFields(), sortFields[ ordinal ] );
        }

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "incomingScope: {}, in pos: {}", incomingScope.getName(), ordinal );
        LOG.debug( "keyFields: {}", printSafe( keyFields[ ordinal ] ) );
        LOG.debug( "valueFields: {}", printSafe( valuesFields[ ordinal ] ) );

        if( sortFields != null )
          LOG.debug( "sortFields: {}", printSafe( sortFields[ ordinal ] ) );
        }
      }

    if( role == Role.sink )
      return;

    keyEntry = new TupleEntry( outgoingScope.getOutGroupingFields(), true );
    tupleEntryIterator = new TupleEntryChainIterator( outgoingScope.getOutValuesFields() );

    grouping = new Grouping<>();
    grouping.key = keyEntry;
    grouping.joinIterator = tupleEntryIterator;
    }

  protected void initComparators()
    {
    Comparator defaultComparator = (Comparator) flowProcess.newInstance( (String) flowProcess.getProperty( FlowProps.DEFAULT_ELEMENT_COMPARATOR ) );

    Fields[] compareFields = new Fields[ getNumDeclaredIncomingBranches() ];
    groupComparators = new Comparator[ getNumDeclaredIncomingBranches() ];

    if( splice.isSorted() )
      valueComparators = new Comparator[ getNumDeclaredIncomingBranches() ];

    int size = splice.isGroupBy() ? 1 : getNumDeclaredIncomingBranches();

    for( int i = 0; i < size; i++ )
      {
      Scope incomingScope = incomingScopes.get( i );

      int pos = splice.isGroupBy() ? 0 : splice.getPipePos().get( incomingScope.getName() );

      // we want the comparators
      Fields groupFields = splice.getKeySelectors().get( incomingScope.getName() );

      compareFields[ pos ] = groupFields; // used for finding hashers

      if( groupFields.size() == 0 )
        groupComparators[ pos ] = groupFields;
      else
        groupComparators[ pos ] = new SparseTupleComparator( Fields.asDeclaration( groupFields ), defaultComparator );

      groupComparators[ pos ] = splice.isSortReversed() ? Collections.reverseOrder( groupComparators[ pos ] ) : groupComparators[ pos ];

      if( sortFields != null )
        {
        // we want the comparators, so don't use sortFields array
        Fields sortFields = splice.getSortingSelectors().get( incomingScope.getName() );
        valueComparators[ pos ] = new SparseTupleComparator( valuesFields[ pos ], sortFields, defaultComparator );

        if( splice.isSortReversed() )
          valueComparators[ pos ] = Collections.reverseOrder( valueComparators[ pos ] );
        }
      }

    nullsAreNotEqual = !areNullsEqual();

    if( nullsAreNotEqual )
      LOG.debug( "treating null values in Tuples at not equal during grouping" );

    Comparator[] hashers = TupleHasher.merge( compareFields );
    groupHasher = defaultComparator != null || !TupleHasher.isNull( hashers ) ? new TupleHasher( defaultComparator, hashers ) : null;
    }

  protected Comparator getKeyComparator()
    {
    if( groupComparators.length > 0 && groupComparators[ 0 ] != null )
      return groupComparators[ 0 ];

    return new Comparator<Comparable>()
    {
    @Override
    public int compare( Comparable lhs, Comparable rhs )
      {
      return lhs.compareTo( rhs );
      }
    };
    }

  @Override
  public FlowElement getFlowElement()
    {
    return splice;
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

  @Override
  public void cleanup()
    {
    super.cleanup();

    // close if top of stack
    if( next == null )
      TrapHandler.closeTraps();
    }

  private boolean areNullsEqual()
    {
    try
      {
      Tuple tupleWithNull = Tuple.size( 1 );

      return groupComparators[ 0 ].compare( tupleWithNull, tupleWithNull ) == 0;
      }
    catch( Exception exception )
      {
      return true; // assume we have an npe or something and they don't expect to see nulls
      }
    }

  protected int getNumDeclaredIncomingBranches()
    {
    return splice.getPrevious().length;
    }

  /**
   * This allows the tuple to honor the hasher and comparators, if any
   *
   * @param object the tuple to wrap
   * @return a DelegatedTuple instance
   */
  protected final Tuple getDelegatedTuple( Tuple object )
    {
    if( groupHasher == null )
      return object;

    return new DelegatedTuple( object );
    }

  private String printSafe( Fields fields )
    {
    if( fields != null )
      return fields.printVerbose();

    return "";
    }

  @Override
  public final boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SpliceGate ) )
      return false;

    SpliceGate spliceGate = (SpliceGate) object;

    if( splice != null ? splice != spliceGate.splice : spliceGate.splice != null )
      return false;

    return true;
    }

  @Override
  public final int hashCode()
    {
    return splice != null ? System.identityHashCode( splice ) : 0;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder( "SpliceGate{" );
    sb.append( "splice=" ).append( splice );
    sb.append( ", role=" ).append( role );
    sb.append( '}' );
    return sb.toString();
    }

  protected class DelegatedTuple extends Tuple
    {
    public DelegatedTuple( Tuple wrapped )
      {
      // pass it in to prevent one being allocated
      super( Tuple.elements( wrapped ) );
      }

    @Override
    public boolean equals( Object object )
      {
      return compareTo( object ) == 0;
      }

    @Override
    public int compareTo( Object other )
      {
      return groupComparators[ 0 ].compare( this, (Tuple) other );
      }

    @Override
    public int hashCode()
      {
      return groupHasher.hashCode( this );
      }
    }
  }
