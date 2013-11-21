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

package cascading.tap.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.TapException;
import cascading.tap.type.FileType;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterableChainIterator;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.tuple.util.TupleViews;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class BasePartitionTap<Config, Input, Output> extends Tap<Config, Input, Output>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( BasePartitionTap.class );
  /** Field OPEN_FILES_THRESHOLD_DEFAULT */
  protected static final int OPEN_WRITES_THRESHOLD_DEFAULT = 300;

  private class PartitionIterator extends TupleEntryIterableChainIterator
    {
    public PartitionIterator( final FlowProcess<Config> flowProcess, Input input ) throws IOException
      {
      super( getSourceFields() );

      List<Iterator<Tuple>> iterators = new ArrayList<Iterator<Tuple>>();

      if( input != null )
        {
        String identifier = parent.getFullIdentifier( flowProcess );
        iterators.add( createPartitionEntryIterator( flowProcess, input, identifier, getCurrentIdentifier( flowProcess ) ) );
        }
      else
        {
        String[] childIdentifiers = getChildPartitionIdentifiers( flowProcess, false );

        for( String childIdentifier : childIdentifiers )
          iterators.add( createPartitionEntryIterator( flowProcess, null, parent.getIdentifier(), childIdentifier ) );
        }

      reset( iterators );
      }

    private PartitionTupleEntryIterator createPartitionEntryIterator( FlowProcess<Config> flowProcess, Input input, String parentIdentifier, String childIdentifier ) throws IOException
      {
      TupleEntrySchemeIterator schemeIterator = createTupleEntrySchemeIterator( flowProcess, parent, childIdentifier, input );

      return new PartitionTupleEntryIterator( getSourceFields(), partition, parentIdentifier, childIdentifier, schemeIterator );
      }
    }

  private class PartitionCollector extends TupleEntryCollector
    {
    private final FlowProcess<Config> flowProcess;
    private final Config conf;
    private final Fields parentFields;
    private final Fields partitionFields;
    private TupleEntry partitionEntry;
    private final Tuple partitionTuple;
    private final Tuple parentTuple;

    public PartitionCollector( FlowProcess<Config> flowProcess )
      {
      super( Fields.asDeclaration( getSinkFields() ) );
      this.flowProcess = flowProcess;
      this.conf = flowProcess.getConfigCopy();
      this.parentFields = parent.getSinkFields();
      this.partitionFields = ( (PartitionScheme) getScheme() ).partitionFields;
      this.partitionEntry = new TupleEntry( this.partitionFields );

      this.partitionTuple = TupleViews.createNarrow( getSinkFields().getPos( this.partitionFields ) );
      this.parentTuple = TupleViews.createNarrow( getSinkFields().getPos( this.parentFields ) );

      this.partitionEntry.setTuple( partitionTuple );
      }

    private TupleEntryCollector getCollector( String path )
      {
      TupleEntryCollector collector = collectors.get( path );

      if( collector != null )
        return collector;

      try
        {
        LOG.debug( "creating collector for parent: {}, path: {}", parent.getFullIdentifier( conf ), path );

        collector = createTupleEntrySchemeCollector( flowProcess, parent, path, openedCollectors );

        openedCollectors++;
        flowProcess.increment( Counters.Paths_Opened, 1 );
        }
      catch( IOException exception )
        {
        throw new TapException( "unable to open partition path: " + path, exception );
        }

      if( collectors.size() > openWritesThreshold )
        purgeCollectors();

      collectors.put( path, collector );

      if( LOG.isInfoEnabled() && collectors.size() % 100 == 0 )
        LOG.info( "caching {} open Taps", collectors.size() );

      return collector;
      }

    private void purgeCollectors()
      {
      int numToClose = Math.max( 1, (int) ( openWritesThreshold * .10 ) );

      if( LOG.isInfoEnabled() )
        LOG.info( "removing {} open Taps from cache of size {}", numToClose, collectors.size() );

      Set<String> removeKeys = new HashSet<String>();
      Set<String> keys = collectors.keySet();

      for( String key : keys )
        {
        if( numToClose-- == 0 )
          break;

        removeKeys.add( key );
        }

      for( String removeKey : removeKeys )
        closeCollector( collectors.remove( removeKey ) );

      flowProcess.increment( Counters.Path_Purges, 1 );
      }

    @Override
    public void close()
      {
      super.close();

      try
        {
        for( TupleEntryCollector collector : collectors.values() )
          closeCollector( collector );
        }
      finally
        {
        collectors.clear();
        }
      }

    private void closeCollector( TupleEntryCollector collector )
      {
      if( collector == null )
        return;

      try
        {
        collector.close();

        flowProcess.increment( Counters.Paths_Closed, 1 );
        }
      catch( Exception exception )
        {
        // do nothing
        }
      }

    protected void collect( TupleEntry tupleEntry ) throws IOException
      {
      // reset select views
      TupleViews.reset( partitionTuple, tupleEntry.getTuple() ); // partitionTuple is inside partitionEntry
      TupleViews.reset( parentTuple, tupleEntry.getTuple() );

      String path = partition.toPartition( partitionEntry );

      getCollector( path ).add( parentTuple );
      }
    }

  /** Field parent */
  protected Tap parent;
  /** Field partition */
  protected Partition partition;
  /** Field keepParentOnDelete */
  protected boolean keepParentOnDelete = false;
  /** Field openTapsThreshold */
  protected int openWritesThreshold = OPEN_WRITES_THRESHOLD_DEFAULT;

  /** Field openedCollectors */
  private long openedCollectors = 0;
  /** Field collectors */
  private final Map<String, TupleEntryCollector> collectors = new LinkedHashMap<String, TupleEntryCollector>( 1000, .75f, true );

  protected abstract TupleEntrySchemeCollector createTupleEntrySchemeCollector( FlowProcess<Config> flowProcess, Tap parent, String path, long sequence ) throws IOException;

  protected abstract TupleEntrySchemeIterator createTupleEntrySchemeIterator( FlowProcess<Config> flowProcess, Tap parent, String path, Input input ) throws IOException;

  public enum Counters
    {
      Paths_Opened, Paths_Closed, Path_Purges
    }

  protected BasePartitionTap( Tap parent, Partition partition, int openWritesThreshold )
    {
    super( new PartitionScheme( parent.getScheme(), partition.getPartitionFields() ), parent.getSinkMode() );
    this.parent = parent;
    this.partition = partition;
    this.openWritesThreshold = openWritesThreshold;
    }

  protected BasePartitionTap( Tap parent, Partition partition, SinkMode sinkMode )
    {
    super( new PartitionScheme( parent.getScheme(), partition.getPartitionFields() ), sinkMode );
    this.parent = parent;
    this.partition = partition;
    }

  protected BasePartitionTap( Tap parent, Partition partition, SinkMode sinkMode, boolean keepParentOnDelete, int openWritesThreshold )
    {
    super( new PartitionScheme( parent.getScheme(), partition.getPartitionFields() ), sinkMode );
    this.parent = parent;
    this.partition = partition;
    this.keepParentOnDelete = keepParentOnDelete;
    this.openWritesThreshold = openWritesThreshold;
    }

  /**
   * Method getParent returns the parent Tap of this PartitionTap object.
   *
   * @return the parent (type Tap) of this PartitionTap object.
   */
  public Tap getParent()
    {
    return parent;
    }

  /**
   * Method getPartition returns the {@link Partition} instance used by this PartitionTap
   *
   * @return the partition instance
   */
  public Partition getPartition()
    {
    return partition;
    }

  /**
   * Method getChildPartitionIdentifiers returns an array of all identifiers for all available partitions.
   * <p/>
   * This method is used internally to set all incoming paths, override to limit applicable partitions.
   * <p/>
   * Note the returns array may be large.
   *
   * @param flowProcess    of type FlowProcess
   * @param fullyQualified of type boolean
   * @return a String[] of partition identifiers
   * @throws IOException
   */
  public String[] getChildPartitionIdentifiers( FlowProcess<Config> flowProcess, boolean fullyQualified ) throws IOException
    {
    return ( (FileType) parent ).getChildIdentifiers( flowProcess.getConfigCopy(), partition.getPathDepth(), fullyQualified );
    }

  @Override
  public String getIdentifier()
    {
    return parent.getIdentifier();
    }

  protected abstract String getCurrentIdentifier( FlowProcess<Config> flowProcess );

  /**
   * Method getOpenWritesThreshold returns the openTapsThreshold of this PartitionTap object.
   *
   * @return the openTapsThreshold (type int) of this PartitionTap object.
   */
  public int getOpenWritesThreshold()
    {
    return openWritesThreshold;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess, Output output ) throws IOException
    {
    return new PartitionCollector( flowProcess );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<Config> flowProcess, Input input ) throws IOException
    {
    return new PartitionIterator( flowProcess, input );
    }

  @Override
  public boolean createResource( Config conf ) throws IOException
    {
    return parent.createResource( conf );
    }

  @Override
  public boolean deleteResource( Config conf ) throws IOException
    {
    return keepParentOnDelete || parent.deleteResource( conf );
    }

  @Override
  public boolean commitResource( Config conf ) throws IOException
    {
    return parent.commitResource( conf );
    }

  @Override
  public boolean rollbackResource( Config conf ) throws IOException
    {
    return parent.rollbackResource( conf );
    }

  @Override
  public boolean resourceExists( Config conf ) throws IOException
    {
    return parent.resourceExists( conf );
    }

  @Override
  public long getModifiedTime( Config conf ) throws IOException
    {
    return parent.getModifiedTime( conf );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    BasePartitionTap that = (BasePartitionTap) object;

    if( parent != null ? !parent.equals( that.parent ) : that.parent != null )
      return false;
    if( partition != null ? !partition.equals( that.partition ) : that.partition != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( parent != null ? parent.hashCode() : 0 );
    result = 31 * result + ( partition != null ? partition.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\"" + partition + "\"]";
    }

  public static class PartitionScheme<Config, Input, Output> extends Scheme<Config, Input, Output, Void, Void>
    {
    private final Scheme scheme;
    private final Fields partitionFields;

    public PartitionScheme( Scheme scheme )
      {
      this.scheme = scheme;
      this.partitionFields = null;
      }

    public PartitionScheme( Scheme scheme, Fields partitionFields )
      {
      this.scheme = scheme;

      if( partitionFields == null || partitionFields.isAll() )
        this.partitionFields = null;
      else if( partitionFields.isDefined() )
        this.partitionFields = partitionFields;
      else
        throw new IllegalArgumentException( "partitionFields must be defined or the ALL substitution, got: " + partitionFields.printVerbose() );
      }

    public Fields getSinkFields()
      {
      if( partitionFields == null || scheme.getSinkFields().isAll() )
        return scheme.getSinkFields();

      return Fields.merge( scheme.getSinkFields(), partitionFields );
      }

    public void setSinkFields( Fields sinkFields )
      {
      scheme.setSinkFields( sinkFields );
      }

    public Fields getSourceFields()
      {
      if( partitionFields == null || scheme.getSourceFields().isUnknown() )
        return scheme.getSourceFields();

      return Fields.merge( scheme.getSourceFields(), partitionFields );
      }

    public void setSourceFields( Fields sourceFields )
      {
      scheme.setSourceFields( sourceFields );
      }

    public int getNumSinkParts()
      {
      return scheme.getNumSinkParts();
      }

    public void setNumSinkParts( int numSinkParts )
      {
      scheme.setNumSinkParts( numSinkParts );
      }

    @Override
    public void sourceConfInit( FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
      {
      scheme.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      scheme.sourcePrepare( flowProcess, sourceCall );
      }

    @Override
    public boolean source( FlowProcess<Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      throw new UnsupportedOperationException( "should never be called" );
      }

    @Override
    public void sourceCleanup( FlowProcess<Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      scheme.sourceCleanup( flowProcess, sourceCall );
      }

    @Override
    public void sinkConfInit( FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
      {
      scheme.sinkConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sinkPrepare( FlowProcess<Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      scheme.sinkPrepare( flowProcess, sinkCall );
      }

    @Override
    public void sink( FlowProcess<Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      throw new UnsupportedOperationException( "should never be called" );
      }

    @Override
    public void sinkCleanup( FlowProcess<Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      scheme.sinkCleanup( flowProcess, sinkCall );
      }
    }
  }
