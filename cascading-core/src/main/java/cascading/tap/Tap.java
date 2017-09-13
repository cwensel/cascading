/*
 * Copyright (c) 2016-2017 Chris K Wensel. All Rights Reserved.
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

package cascading.tap;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.flow.planner.ScopedElement;
import cascading.management.annotation.Property;
import cascading.management.annotation.PropertyDescription;
import cascading.management.annotation.PropertySanitizer;
import cascading.management.annotation.Visibility;
import cascading.pipe.Pipe;
import cascading.property.ConfigDef;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.TraceUtil;
import cascading.util.Traceable;
import cascading.util.Util;

/**
 * A Tap represents the physical data source or sink in a connected {@link cascading.flow.Flow}.
 * </p>
 * That is, a source Tap is the head end of a connected {@link Pipe} and {@link Tuple} stream, and
 * a sink Tap is the tail end. Kinds of Tap types are used to manage files from a local disk,
 * distributed disk, remote storage like Amazon S3, or via FTP. It simply abstracts
 * out the complexity of connecting to these types of data sources.
 * <p/>
 * A Tap takes a {@link Scheme} instance, which is used to identify the type of resource (text file, binary file, etc).
 * A Tap is responsible for how the resource is reached.
 * <p/>
 * By default when planning a Flow, Tap equality is a function of the {@link #getIdentifier()} and {@link #getScheme()}
 * values. That is, two Tap instances are the same Tap instance if they sink/source the same resource and sink/source
 * the same fields.
 * <p/>
 * Some more advanced taps, like a database tap, may need to extend equality to include any filtering, like the
 * {@code where} clause in a SQL statement so two taps reading from the same SQL table aren't considered equal.
 * <p/>
 * Taps are also used to determine dependencies between two or more {@link Flow} instances when used with a
 * {@link cascading.cascade.Cascade}. In that case the {@link #getFullIdentifier(Object)} value is used and the Scheme
 * is ignored.
 */
public abstract class Tap<Config, Input, Output> implements ScopedElement, FlowElement, Serializable, Traceable
  {
  /** Field scheme */
  private Scheme<Config, Input, Output, ?, ?> scheme;

  /** Field mode */
  SinkMode sinkMode = SinkMode.KEEP;

  private ConfigDef configDef;
  private ConfigDef nodeConfigDef;
  private ConfigDef stepConfigDef;

  /** Field id */
  private final String id = Util.createUniqueID(); // 3.0 planner relies on this being consistent
  /** Field trace */
  private String trace = TraceUtil.captureDebugTrace( this ); // see TraceUtil.setTrace() to override

  /**
   * Convenience function to make an array of Tap instances.
   *
   * @param taps of type Tap
   * @return Tap array
   */
  public static Tap[] taps( Tap... taps )
    {
    return taps;
    }

  /**
   * Creates and returns a unique ID for the given Tap, this value is cached and may be used to uniquely identify
   * the Tap instance in properties files etc.
   * <p/>
   * This value is generally reproducible assuming the Tap identifier and the Scheme source and sink Fields remain consistent.
   *
   * @param tap of type Tap
   * @return of type String
   */
  public static synchronized String id( Tap tap )
    {
    if( tap instanceof DecoratorTap )
      return id( ( (DecoratorTap) tap ).getOriginal() );

    return tap.id;
    }

  protected Tap()
    {
    }

  protected Tap( Scheme<Config, Input, Output, ?, ?> scheme )
    {
    this.setScheme( scheme );
    }

  protected Tap( Scheme<Config, Input, Output, ?, ?> scheme, SinkMode sinkMode )
    {
    this.setScheme( scheme );
    this.sinkMode = sinkMode;
    }

  protected void setScheme( Scheme<Config, Input, Output, ?, ?> scheme )
    {
    this.scheme = scheme;
    }

  /**
   * Method getScheme returns the scheme of this Tap object.
   *
   * @return the scheme (type Scheme) of this Tap object.
   */
  public Scheme<Config, Input, Output, ?, ?> getScheme()
    {
    return scheme;
    }

  @Override
  public String getTrace()
    {
    return trace;
    }

  /**
   * Method flowInit allows this Tap instance to initialize itself in context of the given {@link cascading.flow.Flow} instance.
   * This method is guaranteed to be called before the Flow is started and the
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)} event is fired.
   * <p/>
   * This method will be called once per Flow, and before {@link #sourceConfInit(cascading.flow.FlowProcess, Object)} and
   * {@link #sinkConfInit(cascading.flow.FlowProcess, Object)} methods.
   *
   * @param flow of type Flow
   */
  public void flowConfInit( Flow<Config> flow )
    {

    }

  /**
   * Method sourceConfInit initializes this instance as a source.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link cascading.flow.Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   * <p/>
   * Note that no resources or services should be modified by this method.
   *
   * @param flowProcess of type FlowProcess
   * @param conf        of type Config
   */
  public void sourceConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    getScheme().sourceConfInit( flowProcess, this, conf );
    }

  /**
   * Method sinkConfInit initializes this instance as a sink.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link cascading.flow.Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * Note this method will be called in context of this Tap being used as a traditional 'sink' and as a 'trap'.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   * <p/>
   * Note that no resources or services should be modified by this method. If this Tap instance returns true for
   * {@link #isReplace()}, then {@link #deleteResource(Object)} will be called by the parent Flow.
   *
   * @param flowProcess of type FlowProcess
   * @param conf        of type Config
   */
  public void sinkConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    getScheme().sinkConfInit( flowProcess, this, conf );
    }

  /**
   * Method getIdentifier returns a String representing the resource this Tap instance represents.
   * <p/>
   * Often, if the tap accesses a filesystem, the identifier is nothing more than the path to the file or directory.
   * In other cases it may be a an URL or URI representing a connection string or remote resource.
   * <p/>
   * Any two Tap instances having the same value for the identifier are considered equal.
   *
   * @return String
   */
  @Property(name = "identifier", visibility = Visibility.PUBLIC)
  @PropertyDescription("The resource this instance represents")
  @PropertySanitizer("cascading.management.annotation.URISanitizer")
  public abstract String getIdentifier();

  /**
   * Method getSourceFields returns the sourceFields of this Tap object.
   *
   * @return the sourceFields (type Fields) of this Tap object.
   */
  public Fields getSourceFields()
    {
    return getScheme().getSourceFields();
    }

  /**
   * Method getSinkFields returns the sinkFields of this Tap object.
   *
   * @return the sinkFields (type Fields) of this Tap object.
   */
  public Fields getSinkFields()
    {
    return getScheme().getSinkFields();
    }

  /**
   * Method openForRead opens the resource represented by this Tap instance for reading.
   * <p/>
   * {@code input} value may be null, if so, sub-classes must inquire with the underlying {@link Scheme}
   * via {@link Scheme#sourceConfInit(cascading.flow.FlowProcess, Tap, Object)} to get the proper
   * input type and instantiate it before calling {@code super.openForRead()}.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param flowProcess of type FlowProcess
   * @param input       of type Input
   * @return TupleEntryIterator
   * @throws java.io.IOException when the resource cannot be opened
   */
  public abstract TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess, Input input ) throws IOException;

  /**
   * Method openForRead opens the resource represented by this Tap instance for reading.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param flowProcess of type FlowProcess
   * @return TupleEntryIterator
   * @throws java.io.IOException when the resource cannot be opened
   */
  public TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return openForRead( flowProcess, null );
    }

  /**
   * Method openForWrite opens the resource represented by this Tap instance for writing.
   * <p/>
   * This method is used internally and does not honor the {@link SinkMode} setting. If SinkMode is
   * {@link SinkMode#REPLACE}, this call may fail. See {@link #openForWrite(cascading.flow.FlowProcess)}.
   * <p/>
   * {@code output} value may be null, if so, sub-classes must inquire with the underlying {@link Scheme}
   * via {@link Scheme#sinkConfInit(cascading.flow.FlowProcess, Tap, Object)} to get the proper
   * output type and instantiate it before calling {@code super.openForWrite()}.
   *
   * @param flowProcess of type FlowProcess
   * @param output      of type Output
   * @return TupleEntryCollector
   * @throws java.io.IOException when the resource cannot be opened
   */
  public abstract TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess, Output output ) throws IOException;

  /**
   * Method openForWrite opens the resource represented by this Tap instance for writing.
   * <p/>
   * This method is for user application use and does honor the {@link SinkMode#REPLACE} settings. That is, if
   * SinkMode is set to {@link SinkMode#REPLACE} the underlying resource will be deleted.
   * <p/>
   * Note if {@link SinkMode#UPDATE} is set, the resource will not be deleted.
   *
   * @param flowProcess of type FlowProcess
   * @return TupleEntryCollector
   * @throws java.io.IOException when the resource cannot be opened
   */
  public TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    if( isReplace() )
      deleteResource( flowProcess );

    return openForWrite( flowProcess, null );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    // as a source Tap, we emit the scheme defined Fields
    // as a sink Tap, we declare we emit the incoming Fields
    // as a temp Tap, this method never gets called, but we emit what we consume
    int count = 0;
    for( Scope incomingScope : incomingScopes )
      {
      Fields incomingFields = incomingScope.getIncomingTapFields();

      if( incomingFields != null )
        {
        try
          {
          incomingFields.select( getSinkFields() );
          }
        catch( FieldsResolverException exception )
          {
          throw new TapException( this, exception.getSourceFields(), exception.getSelectorFields(), exception );
          }

        count++;
        }
      }

    if( count > 1 )
      throw new FlowException( "Tap may not have more than one incoming Scope" );

    // this allows the incoming to be passed through to the outgoing
    Fields incomingFields = incomingScopes.size() == 0 ? null : incomingScopes.iterator().next().getIncomingTapFields();

    if( incomingFields != null &&
      ( isSource() && getSourceFields().equals( Fields.UNKNOWN ) ||
        isSink() && getSinkFields().equals( Fields.ALL ) ) )
      return new Scope( incomingFields );

    if( count == 1 )
      return new Scope( getSinkFields() );

    return new Scope( getSourceFields() );
    }

  /**
   * A hook for allowing a Scheme to lazily retrieve its source fields.
   *
   * @param flowProcess of type FlowProcess
   * @return the found Fields
   */
  public Fields retrieveSourceFields( FlowProcess<? extends Config> flowProcess )
    {
    return getScheme().retrieveSourceFields( flowProcess, this );
    }

  public void presentSourceFields( FlowProcess<? extends Config> flowProcess, Fields fields )
    {
    getScheme().presentSourceFields( flowProcess, this, fields );
    }

  /**
   * A hook for allowing a Scheme to lazily retrieve its sink fields.
   *
   * @param flowProcess of type FlowProcess
   * @return the found Fields
   */
  public Fields retrieveSinkFields( FlowProcess<? extends Config> flowProcess )
    {
    return getScheme().retrieveSinkFields( flowProcess, this );
    }

  public void presentSinkFields( FlowProcess<? extends Config> flowProcess, Fields fields )
    {
    getScheme().presentSinkFields( flowProcess, this, fields );
    }

  @Override
  public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
    {
    return incomingScope.getIncomingTapFields();
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    return incomingScope.getIncomingTapFields();
    }

  /**
   * Method getFullIdentifier returns a fully qualified resource identifier.
   *
   * @param flowProcess of type FlowProcess
   * @return String
   */
  public String getFullIdentifier( FlowProcess<? extends Config> flowProcess )
    {
    return getFullIdentifier( flowProcess.getConfig() );
    }

  /**
   * Method getFullIdentifier returns a fully qualified resource identifier.
   *
   * @param conf of type Config
   * @return String
   */
  public String getFullIdentifier( Config conf )
    {
    return getIdentifier();
    }

  /**
   * Method createResource creates the underlying resource.
   *
   * @param flowProcess of type FlowProcess
   * @return boolean
   * @throws IOException when there is an error making directories
   */
  public boolean createResource( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return createResource( flowProcess.getConfig() );
    }

  /**
   * Method createResource creates the underlying resource.
   *
   * @param conf of type Config
   * @return boolean
   * @throws IOException when there is an error making directories
   */
  public abstract boolean createResource( Config conf ) throws IOException;

  /**
   * Method deleteResource deletes the resource represented by this instance.
   *
   * @param flowProcess of type FlowProcess
   * @return boolean
   * @throws IOException when the resource cannot be deleted
   */
  public boolean deleteResource( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return deleteResource( flowProcess.getConfig() );
    }

  /**
   * Method deleteResource deletes the resource represented by this instance.
   *
   * @param conf of type Config
   * @return boolean
   * @throws IOException when the resource cannot be deleted
   */
  public abstract boolean deleteResource( Config conf ) throws IOException;

  /**
   * Method prepareResourceForRead allows the underlying resource to be notified when reading will begin.
   * <p/>
   * This method will be called client side so that any remote or external resources can be initialized.
   * <p/>
   * If this method returns {@code false}, an exception will be thrown halting the current Flow.
   * <p/>
   * In most cases, resource initialization should happen in the {@link #openForRead(FlowProcess, Object)}  method.
   * <p/>
   * This allows for initialization of cluster side resources, like a JDBC driver used to read data from a database,
   * that cannot be passed client to cluster.
   *
   * @param conf of type Config
   * @return returns true if successful
   * @throws IOException
   */
  public boolean prepareResourceForRead( Config conf ) throws IOException
    {
    return true;
    }

  /**
   * Method prepareResourceForWrite allows the underlying resource to be notified when writing will begin.
   * <p/>
   * This method will be called once client side so that any remote or external resources can be initialized.
   * <p/>
   * If this method returns {@code false}, an exception will be thrown halting the current Flow.
   * <p/>
   * In most cases, resource initialization should happen in the {@link #openForWrite(FlowProcess, Object)} method.
   * <p/>
   * This allows for initialization of cluster side resources, like a JDBC driver used to write data to a database,
   * that cannot be passed client to cluster.
   * <p/>
   * In the above JDBC example, overriding this method will allow for testing for the existence of and/or creating
   * a remote table used by all individual cluster side tasks.
   *
   * @param conf of type Config
   * @return returns true if successful
   * @throws IOException
   */
  public boolean prepareResourceForWrite( Config conf ) throws IOException
    {
    return true;
    }

  /**
   * Method commitResource allows the underlying resource to be notified when all write processing is
   * successful so that any additional cleanup or processing may be completed.
   * <p/>
   * See {@link #rollbackResource(Object)} to handle cleanup in the face of failures.
   * <p/>
   * This method is invoked once client side and not in the cluster, if any.
   * <p/>
   * If other sink Tap instance in a given Flow fail on commitResource after called on this instance,
   * rollbackResource will not be called.
   *
   * @param conf of type Config
   * @return returns true if successful
   * @throws IOException
   */
  public boolean commitResource( Config conf ) throws IOException
    {
    return true;
    }

  /**
   * Method rollbackResource allows the underlying resource to be notified when any write processing has failed or
   * was stopped so that any cleanup may be started.
   * <p/>
   * See {@link #commitResource(Object)} to handle cleanup when the write has successfully completed.
   * <p/>
   * This method is invoked once client side and not in the cluster, if any.
   *
   * @param conf of type Config
   * @return returns true if successful
   * @throws IOException
   */
  public boolean rollbackResource( Config conf ) throws IOException
    {
    return true;
    }

  /**
   * Method resourceExists returns true if the path represented by this instance exists.
   *
   * @param flowProcess of type FlowProcess
   * @return true if the underlying resource already exists
   * @throws IOException when the status cannot be determined
   */
  public boolean resourceExists( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return resourceExists( flowProcess.getConfig() );
    }

  /**
   * Method resourceExists returns true if the path represented by this instance exists.
   *
   * @param conf of type Config
   * @return true if the underlying resource already exists
   * @throws IOException when the status cannot be determined
   */
  public abstract boolean resourceExists( Config conf ) throws IOException;

  /**
   * Method getModifiedTime returns the date this resource was last modified.
   * <p/>
   * If the resource does not exist, returns zero (0).
   * <p>
   * If the resource is continuous, returns {@link Long#MAX_VALUE}.
   *
   * @param flowProcess of type FlowProcess
   * @return The date this resource was last modified.
   * @throws IOException
   */
  public long getModifiedTime( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return getModifiedTime( flowProcess.getConfig() );
    }

  /**
   * Method getModifiedTime returns the date this resource was last modified.
   * <p/>
   * If the resource does not exist, returns zero (0).
   * <p>
   * If the resource is continuous, returns {@link Long#MAX_VALUE}.
   *
   * @param conf of type Config
   * @return The date this resource was last modified.
   * @throws IOException
   */
  public abstract long getModifiedTime( Config conf ) throws IOException;

  /**
   * Method getSinkMode returns the {@link SinkMode} }of this Tap object.
   *
   * @return the sinkMode (type SinkMode) of this Tap object.
   */
  public SinkMode getSinkMode()
    {
    return sinkMode;
    }

  /**
   * Method isKeep indicates whether the resource represented by this instance should be kept if it
   * already exists when the Flow is started.
   *
   * @return boolean
   */
  public boolean isKeep()
    {
    return sinkMode == SinkMode.KEEP;
    }

  /**
   * Method isReplace indicates whether the resource represented by this instance should be deleted if it
   * already exists when the Flow is started.
   *
   * @return boolean
   */
  public boolean isReplace()
    {
    return sinkMode == SinkMode.REPLACE;
    }

  /**
   * Method isUpdate indicates whether the resource represented by this instance should be updated if it already
   * exists. Otherwise a new resource will be created, via {@link #createResource(Object)}, when the Flow is started.
   *
   * @return boolean
   */
  public boolean isUpdate()
    {
    return sinkMode == SinkMode.UPDATE;
    }

  /**
   * Method isSink returns true if this Tap instance can be used as a sink.
   *
   * @return boolean
   */
  public boolean isSink()
    {
    return getScheme().isSink();
    }

  /**
   * Method isSource returns true if this Tap instance can be used as a source.
   *
   * @return boolean
   */
  public boolean isSource()
    {
    return getScheme().isSource();
    }

  /**
   * Method isTemporary returns true if this Tap is temporary (used for intermediate results).
   *
   * @return the temporary (type boolean) of this Tap object.
   */
  public boolean isTemporary()
    {
    return false;
    }

  /**
   * Returns a {@link cascading.property.ConfigDef} instance that allows for local properties to be set and made available via
   * a resulting {@link cascading.flow.FlowProcess} instance when the tap is invoked.
   * <p/>
   * Any properties set on the configDef will not show up in any {@link Flow} or {@link cascading.flow.FlowStep} process
   * level configuration, but will override any of those values as seen by the current Tap instance method call where a
   * FlowProcess is provided except for the {@link #sourceConfInit(cascading.flow.FlowProcess, Object)} and
   * {@link #sinkConfInit(cascading.flow.FlowProcess, Object)} methods.
   * <p/>
   * That is, the {@code *confInit} methods are called before any ConfigDef is applied, so any values placed into
   * a ConfigDef instance will not be visible to them.
   *
   * @return an instance of ConfigDef
   */
  public ConfigDef getConfigDef()
    {
    if( configDef == null )
      configDef = new ConfigDef();

    return configDef;
    }

  /**
   * Returns {@code true} if there are properties in the configDef instance.
   *
   * @return true if there are configDef properties
   */
  public boolean hasConfigDef()
    {
    return configDef != null && !configDef.isEmpty();
    }

  /**
   * Returns a {@link ConfigDef} instance that allows for process level properties to be set and made available via
   * a resulting {@link cascading.flow.FlowProcess} instance when the tap is invoked.
   * <p/>
   * Any properties set on the nodeConfigDef will not show up in any Flow configuration, but will show up in
   * the current process {@link cascading.flow.FlowNode} (in Apache Tez the Vertex configuration). Any value set in the
   * nodeConfigDef will be overridden by the pipe local {@code #getConfigDef} instance.
   * </p>
   * Use this method to tweak properties in the process node this tap instance is planned into.
   *
   * @return an instance of ConfigDef
   */
  @Override
  public ConfigDef getNodeConfigDef()
    {
    if( nodeConfigDef == null )
      nodeConfigDef = new ConfigDef();

    return nodeConfigDef;
    }

  /**
   * Returns {@code true} if there are properties in the nodeConfigDef instance.
   *
   * @return true if there are nodeConfigDef properties
   */
  @Override
  public boolean hasNodeConfigDef()
    {
    return nodeConfigDef != null && !nodeConfigDef.isEmpty();
    }

  /**
   * Returns a {@link ConfigDef} instance that allows for process level properties to be set and made available via
   * a resulting {@link cascading.flow.FlowProcess} instance when the tap is invoked.
   * <p/>
   * Any properties set on the stepConfigDef will not show up in any Flow configuration, but will show up in
   * the current process {@link cascading.flow.FlowStep} (in Hadoop the MapReduce jobconf). Any value set in the
   * stepConfigDef will be overridden by the tap local {@code #getConfigDef} instance.
   * </p>
   * Use this method to tweak properties in the process step this tap instance is planned into.
   * <p/>
   * Note the {@code *confInit} methods are called before any ConfigDef is applied, so any values placed into
   * a ConfigDef instance will not be visible to them.
   *
   * @return an instance of ConfigDef
   */
  @Override
  public ConfigDef getStepConfigDef()
    {
    if( stepConfigDef == null )
      stepConfigDef = new ConfigDef();

    return stepConfigDef;
    }

  /**
   * Returns {@code true} if there are properties in the stepConfigDef instance.
   *
   * @return true if there are stepConfigDef properties
   */
  @Override
  public boolean hasStepConfigDef()
    {
    return stepConfigDef != null && !stepConfigDef.isEmpty();
    }

  public Spliterator<TupleEntry> spliterator( FlowProcess<? extends Config> flowProcess )
    {
    return splititerator( openForReadUnchecked( flowProcess ) );
    }

  protected TupleEntryIterator openForReadUnchecked( FlowProcess<? extends Config> flowProcess )
    {
    try
      {
      return openForRead( flowProcess );
      }
    catch( IOException exception )
      {
      throw new UncheckedIOException( exception );
      }
    }

  protected Spliterator<TupleEntry> splititerator( TupleEntryIterator iterator )
    {
    return Spliterators.spliteratorUnknownSize( iterator, 0 );
    }

  /**
   * Method entryStream returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link cascading.tuple.TupleEntryStream#entryStream(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @return a Stream of TupleEntry instances
   */
  public Stream<TupleEntry> entryStream( FlowProcess<? extends Config> flowProcess )
    {
    TupleEntryIterator iterator = openForReadUnchecked( flowProcess );
    Spliterator<TupleEntry> spliterator = splititerator( iterator );

    try
      {
      return StreamSupport
        .stream( spliterator, false )
        .onClose( asUncheckedRunnable( iterator ) );
      }
    catch( Error | RuntimeException error )
      {
      try
        {
        iterator.close();
        }
      catch( IOException exception )
        {
        try
          {
          error.addSuppressed( exception );
          }
        catch( Throwable ignore ){}
        }

      throw error;
      }
    }

  /**
   * Method entryStreamCopy returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an TupleEntry instance suitable for caching.
   * <p>
   * Also see {@link cascading.tuple.TupleEntryStream#entryStreamCopy(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @return a Stream of TupleEntry instances
   */
  public Stream<TupleEntry> entryStreamCopy( FlowProcess<? extends Config> flowProcess )
    {
    return entryStream( flowProcess ).map( TupleEntry::new );
    }

  /**
   * Method entryStream returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link cascading.tuple.TupleEntryStream#entryStream(Tap, FlowProcess, Fields)}.
   *
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying TupleEntry
   * @return a Stream of TupleEntry instances
   */
  public Stream<TupleEntry> entryStream( FlowProcess<? extends Config> flowProcess, Fields selector )
    {
    return entryStream( flowProcess ).map( tupleEntry -> tupleEntry.selectEntry( selector ) );
    }

  /**
   * Method entryStreamCopy returns a {@link Stream} of {@link TupleEntry} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link cascading.tuple.TupleEntryStream#entryStreamCopy(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying TupleEntry
   * @return a Stream of TupleEntry instances
   */
  public Stream<TupleEntry> entryStreamCopy( FlowProcess<? extends Config> flowProcess, Fields selector )
    {
    return entryStream( flowProcess ).map( tupleEntry -> tupleEntry.selectEntryCopy( selector ) );
    }

  /**
   * Method tupleStream returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link cascading.tuple.TupleStream#tupleStream(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @return a Stream of Tuple instances
   */
  public Stream<Tuple> tupleStream( FlowProcess<? extends Config> flowProcess )
    {
    return entryStream( flowProcess ).map( TupleEntry::getTuple );
    }

  /**
   * Method tupleStreamCopy returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an Tuple instance suitable for caching.
   * <p>
   * Also see {@link cascading.tuple.TupleStream#tupleStreamCopy(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @return a Stream of Tuple instances
   */
  public Stream<Tuple> tupleStreamCopy( FlowProcess<? extends Config> flowProcess )
    {
    return entryStream( flowProcess ).map( TupleEntry::getTupleCopy );
    }

  /**
   * Method tupleStream returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * Also see {@link cascading.tuple.TupleStream#tupleStream(Tap, FlowProcess, Fields)}.
   *
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying Tuple
   * @return a Stream of TupleE instances
   */
  public Stream<Tuple> tupleStream( FlowProcess<? extends Config> flowProcess, Fields selector )
    {
    return entryStream( flowProcess ).map( tupleEntry -> tupleEntry.selectTuple( selector ) );
    }

  /**
   * Method tupleStreamCopy returns a {@link Stream} of {@link Tuple} instances from the given
   * {@link Tap} instance.
   * <p>
   * This method returns an Tuple instance suitable for caching.
   * <p>
   * Also see {@link cascading.tuple.TupleStream#tupleStreamCopy(Tap, FlowProcess)}.
   *
   * @param flowProcess represents the current platform configuration
   * @param selector    the fields to select from the underlying Tuple
   * @return a Stream of TupleE instances
   */
  public Stream<Tuple> tupleStreamCopy( FlowProcess<? extends Config> flowProcess, Fields selector )
    {
    return entryStream( flowProcess ).map( tupleEntry -> tupleEntry.selectTupleCopy( selector ) );
    }

  private static Runnable asUncheckedRunnable( Closeable closeable )
    {
    return () ->
    {
    try
      {
      closeable.close();
      }
    catch( IOException e )
      {
      throw new UncheckedIOException( e );
      }
    };
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Tap tap = (Tap) object;

    if( getIdentifier() != null ? !getIdentifier().equals( tap.getIdentifier() ) : tap.getIdentifier() != null )
      return false;

    if( getScheme() != null ? !getScheme().equals( tap.getScheme() ) : tap.getScheme() != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = getIdentifier() != null ? getIdentifier().hashCode() : 0;

    result = 31 * result + ( getScheme() != null ? getScheme().hashCode() : 0 );

    return result;
    }

  @Override
  public String toString()
    {
    if( getIdentifier() != null )
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( getIdentifier() ) + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
  }
