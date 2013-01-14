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

package cascading.scheme;

import java.io.IOException;
import java.io.Serializable;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.util.Util;

/**
 * A Scheme defines what is stored in a {@link Tap} instance by declaring the {@link Tuple}
 * field names, and alternately parsing or rendering the incoming or outgoing {@link Tuple}
 * stream, respectively.
 * <p/>
 * A Scheme defines the type of resource data will be sourced from or sinked to.
 * <p/>
 * The default sourceFields are {@link Fields#UNKNOWN} and the default sinkFields are {@link Fields#ALL}.
 * <p/>
 * Any given sourceFields only label the values in the {@link Tuple}s as they are sourced.
 * It does not necessarily filter the output since a given implementation may choose to
 * collapse values and ignore keys depending on the format.
 * <p/>
 * If the sinkFields are {@link Fields#ALL}, the Cascading planner will attempt to resolve the actual field names
 * and make them available via the {@link cascading.scheme.SinkCall#getOutgoingEntry()} method. Sometimes this may
 * not be possible (in the case the {@link Tap#openForWrite(cascading.flow.FlowProcess)} method is called from user
 * code directly (without planner intervention).
 * <p/>
 * If the sinkFields are a valid selector, the {@link #sink(cascading.flow.FlowProcess, SinkCall)} method will
 * only see the fields expected.
 * <p/>
 * Setting the {@code numSinkParts} value to 1 (one) attempts to ensure the output resource has only one part.
 * In the case of MapReduce, this is only a suggestion for the Map side, on the Reduce side it does this by
 * setting the number of reducers to the given value. This may affect performance, so be cautioned.
 * </p>
 * Note that setting numSinkParts does not force the planner to insert a final Reduce operation in the job, so
 * numSinkParts may be ignored entirely if the final job is Map only. To force the Flow to have a final Reduce,
 * add a {@link cascading.pipe.GroupBy} to the assembly before sinking.
 */
public abstract class Scheme<Config, Input, Output, SourceContext, SinkContext> implements Serializable
  {
  /** Field sinkFields */
  Fields sinkFields = Fields.ALL;
  /** Field sourceFields */
  Fields sourceFields = Fields.UNKNOWN;
  /** Field numSinkParts */
  int numSinkParts;
  /** Field trace */
  private final String trace = Util.captureDebugTrace( getClass() );

  /** Constructor Scheme creates a new Scheme instance. */
  protected Scheme()
    {
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sourceFields of type Fields
   */
  protected Scheme( Fields sourceFields )
    {
    setSourceFields( sourceFields );
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sourceFields of type Fields
   * @param numSinkParts of type int
   */
  protected Scheme( Fields sourceFields, int numSinkParts )
    {
    setSourceFields( sourceFields );
    this.numSinkParts = numSinkParts;
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sourceFields of type Fields
   * @param sinkFields   of type Fields
   */
  protected Scheme( Fields sourceFields, Fields sinkFields )
    {
    setSourceFields( sourceFields );
    setSinkFields( sinkFields );
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sourceFields of type Fields
   * @param sinkFields   of type Fields
   * @param numSinkParts of type int
   */
  protected Scheme( Fields sourceFields, Fields sinkFields, int numSinkParts )
    {
    setSourceFields( sourceFields );
    setSinkFields( sinkFields );
    this.numSinkParts = numSinkParts;
    }

  /**
   * Method getSinkFields returns the sinkFields of this Scheme object.
   *
   * @return the sinkFields (type Fields) of this Scheme object.
   */
  public Fields getSinkFields()
    {
    return sinkFields;
    }

  /**
   * Method setSinkFields sets the sinkFields of this Scheme object.
   *
   * @param sinkFields the sinkFields of this Scheme object.
   */
  public void setSinkFields( Fields sinkFields )
    {
    if( sinkFields.isUnknown() )
      this.sinkFields = Fields.ALL;
    else
      this.sinkFields = sinkFields;
    }

  /**
   * Method getSourceFields returns the sourceFields of this Scheme object.
   *
   * @return the sourceFields (type Fields) of this Scheme object.
   */
  public Fields getSourceFields()
    {
    return sourceFields;
    }

  /**
   * Method setSourceFields sets the sourceFields of this Scheme object.
   *
   * @param sourceFields the sourceFields of this Scheme object.
   */
  public void setSourceFields( Fields sourceFields )
    {
    if( sourceFields.isAll() )
      this.sourceFields = Fields.UNKNOWN;
    else
      this.sourceFields = sourceFields;
    }

  /**
   * Method getNumSinkParts returns the numSinkParts of this Scheme object.
   *
   * @return the numSinkParts (type int) of this Scheme object.
   */
  public int getNumSinkParts()
    {
    return numSinkParts;
    }

  /**
   * Method setNumSinkParts sets the numSinkParts of this Scheme object.
   *
   * @param numSinkParts the numSinkParts of this Scheme object.
   */
  public void setNumSinkParts( int numSinkParts )
    {
    this.numSinkParts = numSinkParts;
    }

  /**
   * Method getTrace returns a String that pinpoint where this instance was created for debugging.
   *
   * @return String
   */
  public String getTrace()
    {
    return trace;
    }

  /**
   * Method isSymmetrical returns {@code true} if the sink fields equal the source fields. That is, this
   * scheme sources the same fields as it sinks.
   *
   * @return the symmetrical (type boolean) of this Scheme object.
   */
  public boolean isSymmetrical()
    {
    return getSourceFields().equals( Fields.UNKNOWN ) && getSinkFields().equals( Fields.ALL ) || getSinkFields().equals( getSourceFields() );
    }

  /**
   * Method isSource returns true if this Scheme instance can be used as a source.
   *
   * @return boolean
   */
  public boolean isSource()
    {
    return true;
    }

  /**
   * Method isSink returns true if this Scheme instance can be used as a sink.
   *
   * @return boolean
   */
  public boolean isSink()
    {
    return true;
    }

  /**
   * Method retrieveSourceFields notifies a Scheme when it is appropriate to dynamically
   * update the fields it sources. By default the current declared fields are returned.
   * <p/>
   * The {@code FlowProcess} presents all known properties resolved by the current planner.
   * <p/>
   * The {@code tap} instance is the parent {@link Tap} for this Scheme instance.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @return Fields
   */
  public Fields retrieveSourceFields( FlowProcess<Config> flowProcess, Tap tap )
    {
    return getSourceFields();
    }

  /**
   * Method presentSourceFields is called after the planner is invoked and all fields are resolved. This
   * method presents to the Scheme the actual source fields after any planner intervention.
   * <p/>
   * This method is called after {@link #retrieveSourceFields(cascading.flow.FlowProcess, cascading.tap.Tap)}.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @param fields      of type Fields
   */
  public void presentSourceFields( FlowProcess<Config> flowProcess, Tap tap, Fields fields )
    {
    presentSourceFieldsInternal( fields );
    }

  protected void presentSourceFieldsInternal( Fields fields )
    {
    if( getSourceFields().equals( Fields.UNKNOWN ) )
      setSourceFields( fields );
    }

  /**
   * Method retrieveSinkFields notifies a Scheme when it is appropriate to dynamically
   * update the fields it sources. By default the current declared fields are returned.
   * <p/>
   * The {@code FlowProcess} presents all known properties resolved by the current planner.
   * <p/>
   * The {@code tap} instance is the parent {@link Tap} for this Scheme instance.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @return Fields
   */
  public Fields retrieveSinkFields( FlowProcess<Config> flowProcess, Tap tap )
    {
    return getSinkFields();
    }

  /**
   * Method presentSinkFields is called after the planner is invoked and all fields are resolved. This
   * method presents to the Scheme the actual source fields after any planner intervention.
   * <p/>
   * This method is called after {@link #retrieveSinkFields(cascading.flow.FlowProcess, cascading.tap.Tap)}.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @param fields      of type Fields
   */
  public void presentSinkFields( FlowProcess<Config> flowProcess, Tap tap, Fields fields )
    {
    presentSinkFieldsInternal( fields );
    }

  protected void presentSinkFieldsInternal( Fields fields )
    {
    if( getSinkFields().equals( Fields.ALL ) )
      setSinkFields( fields );
    }

  /**
   * Method sourceInit initializes this instance as a source.
   * <p/>
   * This method is executed client side as a means to provide necessary configuration parameters
   * used by the underlying platform.
   * <p/>
   * It is not intended to initialize resources that would be necessary during the execution of this
   * class, like a "formatter" or "parser".
   * <p/>
   * See {@link #sourcePrepare(cascading.flow.FlowProcess, SourceCall)} if resources much be initialized
   * before use. And {@link #sourceCleanup(cascading.flow.FlowProcess, SourceCall)} if resources must be
   * destroyed after use.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @param conf        of type Config
   */
  public abstract void sourceConfInit( FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf );

  /**
   * Method sinkInit initializes this instance as a sink.
   * <p/>
   * This method is executed client side as a means to provide necessary configuration parameters
   * used by the underlying platform.
   * <p/>
   * It is not intended to initialize resources that would be necessary during the execution of this
   * class, like a "formatter" or "parser".
   * <p/>
   * See {@link #sinkPrepare(cascading.flow.FlowProcess, SinkCall)} if resources much be initialized
   * before use. And {@link #sinkCleanup(cascading.flow.FlowProcess, SinkCall)} if resources must be
   * destroyed after use.
   *
   * @param flowProcess of type FlowProcess
   * @param tap         of type Tap
   * @param conf        of type Config
   */
  public abstract void sinkConfInit( FlowProcess<Config> flowProcess, Tap<Config, Input, Output> tap, Config conf );

  /**
   * Method sourcePrepare is used to initialize resources needed during each call of
   * {@link #source(cascading.flow.FlowProcess, SourceCall)}.
   * <p/>
   * Be sure to place any initialized objects in the {@code SourceContext} so each instance
   * will remain threadsafe.
   *
   * @param flowProcess of type FlowProcess
   * @param sourceCall  of type SourceCall<SourceContext, Input>
   */
  public void sourcePrepare( FlowProcess<Config> flowProcess, SourceCall<SourceContext, Input> sourceCall ) throws IOException
    {
    }

  /**
   * Method source will read a new "record" or value from {@link cascading.scheme.SourceCall#getInput()} and populate
   * the available {@link Tuple} via {@link cascading.scheme.SourceCall#getIncomingEntry()} and return {@code true}
   * on success or {@code false} if no more values available.
   * <p/>
   * It's ok to set a new Tuple instance on the {@code incomingEntry} {@link cascading.tuple.TupleEntry}, or
   * to simply re-use the existing instance.
   * <p/>
   * Note this is only time it is safe to modify a Tuple instance handed over via a method call.
   * <p/>
   * This method may optionally throw a {@link cascading.tap.TapException} if it cannot process a particular
   * instance of data. If the payload Tuple is set on the TapException, that Tuple will be written to
   * any applicable failure trap Tap.
   *
   * @param flowProcess of type FlowProcess
   * @param sourceCall  of SourceCall
   * @return returns {@code true} when a Tuple was successfully read
   */
  public abstract boolean source( FlowProcess<Config> flowProcess, SourceCall<SourceContext, Input> sourceCall ) throws IOException;

  /**
   * Method sourceCleanup is used to destroy resources created by
   * {@link #sourcePrepare(cascading.flow.FlowProcess, SourceCall)}.
   *
   * @param flowProcess of Process
   * @param sourceCall  of type SourceCall<SourceContext, Input>
   */
  public void sourceCleanup( FlowProcess<Config> flowProcess, SourceCall<SourceContext, Input> sourceCall ) throws IOException
    {
    }

  /**
   * Method sinkPrepare is used to initialize resources needed during each call of
   * {@link #sink(cascading.flow.FlowProcess, SinkCall)}.
   * <p/>
   * Be sure to place any initialized objects in the {@code SinkContext} so each instance
   * will remain threadsafe.
   *
   * @param flowProcess of type FlowProcess
   * @param sinkCall    of type SinkCall<SinkContext, Output>
   */
  public void sinkPrepare( FlowProcess<Config> flowProcess, SinkCall<SinkContext, Output> sinkCall ) throws IOException
    {
    }

  /**
   * Method sink writes out the given {@link Tuple} found on {@link cascading.scheme.SinkCall#getOutgoingEntry()} to
   * the {@link cascading.scheme.SinkCall#getOutput()}.
   * <p/>
   * This method may optionally throw a {@link cascading.tap.TapException} if it cannot process a particular
   * instance of data. If the payload Tuple is set on the TapException, that Tuple will be written to
   * any applicable failure trap Tap. If not set, the incoming Tuple will be written instead.
   *
   * @param flowProcess of Process
   * @param sinkCall    of SinkCall
   */
  public abstract void sink( FlowProcess<Config> flowProcess, SinkCall<SinkContext, Output> sinkCall ) throws IOException;

  /**
   * Method sinkCleanup is used to destroy resources created by
   * {@link #sinkPrepare(cascading.flow.FlowProcess, SinkCall)}.
   *
   * @param flowProcess of type FlowProcess
   * @param sinkCall    of type SinkCall<SinkContext, Output>
   */
  public void sinkCleanup( FlowProcess<Config> flowProcess, SinkCall<SinkContext, Output> sinkCall ) throws IOException
    {
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Scheme scheme = (Scheme) object;

    if( numSinkParts != scheme.numSinkParts )
      return false;
    if( sinkFields != null ? !sinkFields.equals( scheme.sinkFields ) : scheme.sinkFields != null )
      return false;
    if( sourceFields != null ? !sourceFields.equals( scheme.sourceFields ) : scheme.sourceFields != null )
      return false;

    return true;
    }

  @Override
  public String toString()
    {
    if( getSinkFields().equals( getSourceFields() ) )
      return getClass().getSimpleName() + "[" + getSourceFields().print() + "]";
    else
      return getClass().getSimpleName() + "[" + getSourceFields().print() + "->" + getSinkFields().print() + "]";
    }

  public int hashCode()
    {
    int result;
    result = sinkFields != null ? sinkFields.hashCode() : 0;
    result = 31 * result + ( sourceFields != null ? sourceFields.hashCode() : 0 );
    result = 31 * result + numSinkParts;
    return result;
    }
  }
