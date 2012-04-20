/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.pipe.Pipe;
import cascading.property.ConfigDef;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
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
 * A Tap is not given an explicit name by design. This is so a given Tap instance can be
 * re-used in different {@link cascading.flow.Flow}s that may expect a source or sink by a different
 * logical name, but are the same physical resource. If a tap had a name other than its path, which would be
 * used for the tap identity? If the name, then two Tap instances with different names but the same path could
 * interfere with one another.
 */
public abstract class Tap<Process extends FlowProcess<Config>, Config, Input, Output> implements FlowElement, Serializable
  {
  /** Field scheme */
  private Scheme scheme;

  /** Field mode */
  SinkMode sinkMode = SinkMode.KEEP;

  private ConfigDef configDef;

  private ConfigDef processConfigDef;

  /** Field trace */
  private final String trace = Util.captureDebugTrace( getClass() );

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

  protected Tap()
    {
    }

  protected Tap( Scheme scheme )
    {
    this.setScheme( scheme );
    }

  protected Tap( Scheme scheme, SinkMode sinkMode )
    {
    this.setScheme( scheme );
    this.sinkMode = sinkMode;
    }

  protected void setScheme( Scheme scheme )
    {
    this.scheme = scheme;
    }

  /**
   * Method getScheme returns the scheme of this Tap object.
   *
   * @return the scheme (type Scheme) of this Tap object.
   */
  public Scheme getScheme()
    {
    return scheme;
    }

  /**
   * Method getTrace return the trace of this object.
   *
   * @return String
   */
  public String getTrace()
    {
    return trace;
    }

  /**
   * Method flowInit allows this Tap instance to initialize itself in context of the given {@link cascading.flow.Flow} instance.
   * This method is guaranteed to be called before the Flow is started and the
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)} event is fired.
   * <p/>
   * This method will be called once per Flow, and before {@link #sourceConfInit(Process, Config)} and
   * {@link #sinkConfInit(Process, Config)} methods.
   *
   * @param flow of type Flow
   */
  public void flowConfInit( Flow flow )
    {

    }

  /**
   * Method sourceInit initializes this instance as a source.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link cascading.flow.Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   *
   * @param process
   * @param conf    of type JobConf  @throws IOException on resource initialization failure.
   */
  public void sourceConfInit( Process process, Config conf )
    {
    getScheme().sourceConfInit( process, this, conf );
    }

  /**
   * Method sinkInit initializes this instance as a sink.
   * <p/>
   * This method maybe called more than once if this Tap instance is used outside the scope of a {@link cascading.flow.Flow}
   * instance or if it participates in multiple times in a given Flow or across different Flows in
   * a {@link cascading.cascade.Cascade}.
   * <p/>
   * Note this method will be called in context of this Tap being used as a traditional 'sink' and as a 'trap'.
   * <p/>
   * In the context of a Flow, it will be called after
   * {@link cascading.flow.FlowListener#onStarting(cascading.flow.Flow)}
   *
   * @param process
   * @param conf    of type JobConf  @throws IOException on resource initialization failure.
   */
  public void sinkConfInit( Process process, Config conf )
    {
    getScheme().sinkConfInit( process, this, conf );
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
   * Method openForRead opens the resource represented by this Tap instance.
   * <p/>
   * {@code input} value may be null, if so, sub-classes must inquire with the underlying {@link Scheme}
   * via {@link Scheme#sourceConfInit(cascading.flow.FlowProcess, Tap, Object)} to get the proper
   * input type and instantiate it before calling {@code super.openForRead()}.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param flowProcess
   * @param input
   * @return TupleEntryIterator  @throws java.io.IOException when the resource cannot be opened
   */
  public abstract TupleEntryIterator openForRead( Process flowProcess, Input input ) throws IOException;

  /**
   * Method openForRead opens the resource represented by this Tap instance.
   * <p/>
   * Note the returned iterator will return the same instance of {@link cascading.tuple.TupleEntry} on every call,
   * thus a copy must be made of either the TupleEntry or the underlying {@code Tuple} instance if they are to be
   * stored in a Collection.
   *
   * @param flowProcess
   * @return TupleEntryIterator  @throws java.io.IOException when the resource cannot be opened
   */
  public TupleEntryIterator openForRead( Process flowProcess ) throws IOException
    {
    return openForRead( flowProcess, null );
    }

  /**
   * Method openForWrite opens the resource represented by this Tap instance.
   * <p/>
   * {@code output} value may be null, if so, sub-classes must inquire with the underlying {@link Scheme}
   * via {@link Scheme#sinkConfInit(cascading.flow.FlowProcess, Tap, Object)} to get the proper
   * output type and instantiate it before calling {@code super.openForWrite()}.
   *
   * @param flowProcess
   * @param output
   * @return TupleEntryCollector
   * @throws java.io.IOException when
   */
  public abstract TupleEntryCollector openForWrite( Process flowProcess, Output output ) throws IOException;

  /**
   * Method openForWrite opens the resource represented by this Tap instance.
   *
   * @param flowProcess
   * @return TupleEntryCollector
   * @throws java.io.IOException when
   */
  public TupleEntryCollector openForWrite( Process flowProcess ) throws IOException
    {
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
      Fields incomingFields = resolveFields( incomingScope );

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

    if( count == 1 )
      return new Scope( getSinkFields() );

    return new Scope( getSourceFields() );
    }

  /**
   * A hook for allowing a Scheme to lazily retrieve its source fields.
   *
   * @param flowProcess
   * @return
   */
  public Fields retrieveSourceFields( FlowProcess<Config> flowProcess )
    {
    return getScheme().retrieveSourceFields( flowProcess, this );
    }

  public void presentSourceFields( FlowProcess<Config> flowProcess, Fields fields )
    {
    getScheme().presentSourceFields( flowProcess, this, fields );
    }

  /**
   * A hook for allowing a Scheme to lazily retrieve its sink fields.
   *
   * @return
   */
  public Fields retrieveSinkFields( FlowProcess<Config> flowProcess )
    {
    return getScheme().retrieveSinkFields( flowProcess, this );
    }

  public void presentSinkFields( FlowProcess<Config> flowProcess, Fields fields )
    {
    getScheme().presentSinkFields( flowProcess, this, fields );
    }

  @Override
  public Fields resolveIncomingOperationFields( Scope incomingScope )
    {
    return getFieldsFor( incomingScope );
    }

  @Override
  public Fields resolveFields( Scope scope )
    {
    return getFieldsFor( scope );
    }

  private Fields getFieldsFor( Scope incomingScope )
    {
    if( incomingScope.isEvery() )
      return incomingScope.getOutGroupingFields();
    else
      return incomingScope.getOutValuesFields();
    }

  /**
   * Method getFullIdentifier returns a fully qualified resource identifier.
   *
   * @param conf of type Config
   * @return Path
   */
  public String getFullIdentifier( Config conf )
    {
    return getIdentifier();
    }

  /**
   * Method createResource creates the underlying resource.
   *
   * @param conf of type JobConf
   * @return boolean
   * @throws IOException when there is an error making directories
   */
  public abstract boolean createResource( Config conf ) throws IOException;

  /**
   * Method deleteResource deletes the resource represented by this instance.
   *
   * @param conf of type JobConf
   * @return boolean
   * @throws IOException when the resource cannot be deleted
   */
  public abstract boolean deleteResource( Config conf ) throws IOException;

  /**
   * Method commitResource allows the underlying resource to be notified when all write processing is
   * successful so that any additional cleanup or processing may be completed.
   * <p/>
   * See {@link #rollbackResource(Object)} to handle cleanup in the face of failures.
   * <p/>
   * This method is invoked once "client side" and not in the cluster, if any.
   * <p/>
   * <emphasis>This is an experimental API and subject to refinement!!</emphasis>
   *
   * @param conf
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
   * This method is invoked once "client side" and not in the cluster, if any.
   * <p/>
   * <emphasis>This is an experimental API and subject to refinement!!</emphasis>
   *
   * @param conf
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
   * @param conf of type JobConf
   * @return true if the underlying resource already exists
   * @throws IOException when the status cannot be determined
   */
  public abstract boolean resourceExists( Config conf ) throws IOException;

  /**
   * Method getModifiedTime returns the date this resource was last modified.
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
   * exists. Otherwise a new resource will be created when the Flow is started..
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
   * Any properties set on the processConfigDef will not show up in any Flow configuration, but will show up in
   * the current process {@link cascading.flow.FlowStep} (in Hadoop the MapReduce jobconf). Any value set in the
   * processConfigDef will be overridden by the tap local {@code #getConfigDef} instance.
   * </p>
   * Use this method to tweak properties in the process step this tap instance is planned into.
   * <p/>
   * Note the {@code *confInit} methods are called before any ConfigDef is applied, so any values placed into
   * a ConfigDef instance will not be visible to them.
   *
   * @return an instance of ConfigDef
   */
  @Override
  public ConfigDef getProcessConfigDef()
    {
    if( processConfigDef == null )
      processConfigDef = new ConfigDef();

    return processConfigDef;
    }

  /**
   * Returns {@code true} if there are properties in the processConfigDef instance.
   *
   * @return true if there are processConfigDef properties
   */
  @Override
  public boolean hasProcessConfigDef()
    {
    return processConfigDef != null && !processConfigDef.isEmpty();
    }

  @Override
  public boolean isEquivalentTo( FlowElement element )
    {
    if( element == null )
      return false;

    if( this == element )
      return true;

    boolean compare = getClass() == element.getClass();

    if( !compare )
      return false;

    return equals( element );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Tap tap = (Tap) object;

    if( getScheme() != null ? !getScheme().equals( tap.getScheme() ) : tap.getScheme() != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    return getScheme() != null ? getScheme().hashCode() : 0;
    }
  }
