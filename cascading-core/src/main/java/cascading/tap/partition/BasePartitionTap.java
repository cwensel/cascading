/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.Filter;
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
public abstract class BasePartitionTap<Config, Input, Output> extends Tap<Config, Input, Output> implements FileType<Config>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( BasePartitionTap.class );
  /** Field OPEN_FILES_THRESHOLD_DEFAULT */
  protected static final int OPEN_WRITES_THRESHOLD_DEFAULT = 300;

  private class PartitionIterator extends TupleEntryIterableChainIterator
    {
    public PartitionIterator( final FlowProcess<? extends Config> flowProcess, Input input ) throws IOException
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

    private PartitionTupleEntryIterator createPartitionEntryIterator( FlowProcess<? extends Config> flowProcess, Input input, String parentIdentifier, String childIdentifier ) throws IOException
      {
      TupleEntrySchemeIterator schemeIterator = createTupleEntrySchemeIterator( flowProcess, parent, childIdentifier, input );

      return new PartitionTupleEntryIterator( getSourceFields(), partition, parentIdentifier, childIdentifier, schemeIterator );
      }
    }

  public class PartitionCollector extends TupleEntryCollector
    {
    private final FlowProcess<? extends Config> flowProcess;
    private final Config conf;
    private final Fields parentFields;
    private final Fields partitionFields;
    private TupleEntry partitionEntry;
    private final Tuple partitionTuple;
    private final Tuple parentTuple;

    public PartitionCollector( FlowProcess<? extends Config> flowProcess )
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

    TupleEntryCollector getCollector( String path )
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
        {
        closeCollector( removeKey );
        collectors.remove( removeKey );
        }

      flowProcess.increment( Counters.Path_Purges, 1 );
      }

    @Override
    public void close()
      {
      super.close();

      try
        {
        for( String path : new ArrayList<String>( collectors.keySet() ) )
          closeCollector( path );
        }
      finally
        {
        collectors.clear();
        }
      }

    public void closeCollector( String path )
      {
      TupleEntryCollector collector = collectors.get( path );
      if( collector == null )
        return;
      try
        {
        collector.close();

        flowProcess.increment( Counters.Paths_Closed, 1 );
        }
      catch( Exception exception )
        {
        LOG.error( "exception while closing TupleEntryCollector {}", path, exception );

        boolean failOnError = false;
        Object failProperty = flowProcess.getProperty( PartitionTapProps.FAIL_ON_CLOSE );

        if( failProperty != null )
          failOnError = Boolean.parseBoolean( failProperty.toString() );

        if( failOnError )
          throw new TapException( exception );
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
  /** Field sourcePartitionFilters */
  protected final List<PartitionTapFilter> sourcePartitionFilters = new ArrayList<>();
  /** Field keepParentOnDelete */
  protected boolean keepParentOnDelete = false;
  /** Field openTapsThreshold */
  protected int openWritesThreshold = OPEN_WRITES_THRESHOLD_DEFAULT;

  /** Field openedCollectors */
  private long openedCollectors = 0;
  /** Field collectors */
  private final Map<String, TupleEntryCollector> collectors = new LinkedHashMap<String, TupleEntryCollector>( 1000, .75f, true );

  protected abstract TupleEntrySchemeCollector createTupleEntrySchemeCollector( FlowProcess<? extends Config> flowProcess, Tap parent, String path, long sequence ) throws IOException;

  protected abstract TupleEntrySchemeIterator createTupleEntrySchemeIterator( FlowProcess<? extends Config> flowProcess, Tap parent, String path, Input input ) throws IOException;

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
   * <p>
   * This method is used internally to set all incoming paths, override to limit applicable partitions.
   * <p>
   * Note the returns array may be large.
   *
   * @param flowProcess    of type FlowProcess
   * @param fullyQualified of type boolean
   * @return a String[] of partition identifiers
   * @throws IOException
   */
  public String[] getChildPartitionIdentifiers( FlowProcess<? extends Config> flowProcess, boolean fullyQualified ) throws IOException
    {
    String[] childIdentifiers = ( castFileType() ).getChildIdentifiers(
      flowProcess.getConfig(),
      partition.getPathDepth(),
      fullyQualified
    );

    if( sourcePartitionFilters.isEmpty() )
      return childIdentifiers;

    return getFilteredPartitionIdentifiers( flowProcess, childIdentifiers );
    }

  protected String[] getFilteredPartitionIdentifiers( FlowProcess<? extends Config> flowProcess, String[] childIdentifiers )
    {
    Fields partitionFields = partition.getPartitionFields();
    TupleEntry partitionEntry = new TupleEntry( partitionFields, Tuple.size( partitionFields.size() ) );

    List<String> filteredIdentifiers = new ArrayList<>( childIdentifiers.length );

    for( PartitionTapFilter filter : sourcePartitionFilters )
      filter.prepare( flowProcess );

    for( String childIdentifier : childIdentifiers )
      {
      partition.toTuple( childIdentifier.substring( parent.getFullIdentifier( flowProcess ).length() + 1 ), partitionEntry );

      boolean isRemove = false;
      for( PartitionTapFilter filter : sourcePartitionFilters )
        {
        if( filter.isRemove( flowProcess, partitionEntry ) )
          {
          isRemove = true;
          break;
          }
        }

      if( !isRemove )
        filteredIdentifiers.add( childIdentifier );
      }

    for( PartitionTapFilter filter : sourcePartitionFilters )
      filter.cleanup( flowProcess );

    return filteredIdentifiers.toArray( new String[ filteredIdentifiers.size() ] );
    }

  /**
   * Add a {@link Filter} with its associated argument selector when using this PartitionTap as a source. On read, each
   * child identifier is converted to a {@link Tuple} using the provided {@link Partition}. Each {@link Filter} will be
   * applied to the {@link Tuple} so that the input paths can be filtered to only accept those required for the
   * {@link Flow}.
   *
   * @param argumentSelector field selector that selects Filter arguments from the input Tuple
   * @param filter           Filter to be applied to each input Tuple
   */
  public void addSourcePartitionFilter( Fields argumentSelector, Filter filter )
    {
    Fields argumentFields;

    if( argumentSelector.isAll() )
      argumentFields = partition.getPartitionFields();
    else
      argumentFields = partition.getPartitionFields().select( argumentSelector );

    sourcePartitionFilters.add( new PartitionTapFilter( argumentFields, filter ) );
    }

  @Override
  public String getIdentifier()
    {
    return parent.getIdentifier();
    }

  protected abstract String getCurrentIdentifier( FlowProcess<? extends Config> flowProcess );

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
  public TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess, Output output ) throws IOException
    {
    return new PartitionCollector( flowProcess );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess, Input input ) throws IOException
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
  public boolean prepareResourceForRead( Config conf ) throws IOException
    {
    return parent.prepareResourceForRead( conf );
    }

  @Override
  public boolean prepareResourceForWrite( Config conf ) throws IOException
    {
    return parent.prepareResourceForWrite( conf );
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
  public boolean isDirectory( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return castFileType().isDirectory( flowProcess );
    }

  @Override
  public boolean isDirectory( Config conf ) throws IOException
    {
    return castFileType().isDirectory( conf );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return castFileType().getChildIdentifiers( flowProcess );
    }

  @Override
  public String[] getChildIdentifiers( Config conf ) throws IOException
    {
    return castFileType().getChildIdentifiers( conf );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Config> flowProcess, int depth, boolean fullyQualified ) throws IOException
    {
    return castFileType().getChildIdentifiers( flowProcess, depth, fullyQualified );
    }

  @Override
  public String[] getChildIdentifiers( Config conf, int depth, boolean fullyQualified ) throws IOException
    {
    return getChildIdentifiers( conf, depth, fullyQualified );
    }

  @Override
  public long getSize( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return castFileType().getSize( flowProcess );
    }

  @Override
  public long getSize( Config conf ) throws IOException
    {
    return castFileType().getSize( conf );
    }

  protected FileType<Config> castFileType()
    {
    if( parent instanceof FileType )
      return (FileType<Config>) parent;

    throw new UnsupportedOperationException( "parent is not an implementation of " + FileType.class.getName() + ", is type: " + parent.getClass().getName() );
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
    if( partition != null ? !sourcePartitionFilters.equals( that.sourcePartitionFilters ) : that.sourcePartitionFilters != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( parent != null ? parent.hashCode() : 0 );
    result = 31 * result + ( partition != null ? partition.hashCode() : 0 );
    result = 31 * result + ( sourcePartitionFilters != null ? sourcePartitionFilters.hashCode() : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[\"" + parent + "\"]" + "[\"" + partition + "\"]" + "[\"" + sourcePartitionFilters + "\"]";
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

    @Override
    public Fields retrieveSourceFields( FlowProcess<? extends Config> flowProcess, Tap tap )
      {
      return scheme.retrieveSourceFields( flowProcess, tap );
      }

    @Override
    public Fields retrieveSinkFields( FlowProcess<? extends Config> flowProcess, Tap tap )
      {
      return scheme.retrieveSinkFields( flowProcess, tap );
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
    public void sourceConfInit( FlowProcess<? extends Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
      {
      scheme.sourceConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sourcePrepare( FlowProcess<? extends Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      scheme.sourcePrepare( flowProcess, sourceCall );
      }

    @Override
    public boolean source( FlowProcess<? extends Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      throw new UnsupportedOperationException( "should never be called" );
      }

    @Override
    public void sourceCleanup( FlowProcess<? extends Config> flowProcess, SourceCall<Void, Input> sourceCall ) throws IOException
      {
      scheme.sourceCleanup( flowProcess, sourceCall );
      }

    @Override
    public void sinkConfInit( FlowProcess<? extends Config> flowProcess, Tap<Config, Input, Output> tap, Config conf )
      {
      scheme.sinkConfInit( flowProcess, tap, conf );
      }

    @Override
    public void sinkPrepare( FlowProcess<? extends Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      scheme.sinkPrepare( flowProcess, sinkCall );
      }

    @Override
    public void sink( FlowProcess<? extends Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      throw new UnsupportedOperationException( "should never be called" );
      }

    @Override
    public void sinkCleanup( FlowProcess<? extends Config> flowProcess, SinkCall<Void, Output> sinkCall ) throws IOException
      {
      scheme.sinkCleanup( flowProcess, sinkCall );
      }
    }
  }
