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

package cascading.scheme;

import java.io.IOException;
import java.io.Serializable;

import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;

/**
 * A Scheme defines what is stored in a {@link Tap} instance by declaring the {@link Tuple}
 * field names, and alternately parsing or rendering the incoming or outgoing {@link Tuple}
 * stream, respectively.
 * <p/>
 * A Scheme defines the type of resource data will be sourced from or sinked to.
 * <p/>
 * The given fieldNames only label the values in the {@link Tuple}s as they are sourced.
 * It does not necessarily filter the output since a given implemenation may choose to
 * collapse values and ignore keys depending on the format.
 * <p/>
 * Setting the {@code numSinkParts} value insures the output resource has only one part. It does this by
 * setting the number of reducers to the given value. This may affect performance, so be cautioned.
 */
public abstract class Scheme implements Serializable
  {
  /** Field sinkFields */
  Fields sinkFields = Fields.ALL;
  /** Field sourceFields */
  Fields sourceFields;
  /** Field numSinkParts */
  int numSinkParts;

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
    this.sourceFields = sourceFields;
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sourceFields of type Fields
   * @param numSinkParts of type int
   */
  protected Scheme( Fields sourceFields, int numSinkParts )
    {
    this.sourceFields = sourceFields;
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
    this.sourceFields = sourceFields;
    this.sinkFields = sinkFields;
    }

  /**
   * Constructor Scheme creates a new Scheme instance.
   *
   * @param sinkFields   of type Fields
   * @param sourceFields of type Fields
   * @param numSinkParts of type int
   */
  protected Scheme( Fields sinkFields, Fields sourceFields, int numSinkParts )
    {
    this.sinkFields = sinkFields;
    this.sourceFields = sourceFields;
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
   * Method sourceInit initializes this instance as a source.
   *
   * @param tap
   * @param conf of type JobConf
   */
  public abstract void sourceInit( Tap tap, JobConf conf ) throws IOException;

  /**
   * Method sinkInit initializes this instance as a sink.
   *
   * @param tap
   * @param conf of type JobConf
   */
  public abstract void sinkInit( Tap tap, JobConf conf ) throws IOException;

  /**
   * Method source takes the given Hadoop key and value and returns a new {@link Tuple} instance.
   *
   * @param key   of type WritableComparable
   * @param value of type Writable
   * @return Tuple
   */
  public abstract Tuple source( WritableComparable key, Writable value );

  /**
   * Method getInputFormat returns the inputFormat of this Scheme object.
   *
   * @param conf
   * @return the inputFormat (type InputFormat) of this Scheme object.
   */
  public abstract InputFormat getInputFormat( JobConf conf );

  public abstract OutputFormat getOutputFormat( JobConf conf );

  /**
   * Method sink writes out the given {@link Tuple} instance to the outputCollector.
   *
   * @param fields          of type Fields
   * @param tuple           of type Tuple
   * @param outputCollector of type OutputCollector
   * @throws IOException when
   */
  public abstract void sink( Fields fields, Tuple tuple, OutputCollector outputCollector ) throws IOException;


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

  public int hashCode()
    {
    int result;
    result = ( sinkFields != null ? sinkFields.hashCode() : 0 );
    result = 31 * result + ( sourceFields != null ? sourceFields.hashCode() : 0 );
    result = 31 * result + numSinkParts;
    return result;
    }
  }
