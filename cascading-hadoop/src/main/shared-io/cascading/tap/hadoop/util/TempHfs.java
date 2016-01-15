/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop.util;

import java.io.IOException;
import java.util.Set;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/** Class TempHfs creates a temporary {@link cascading.tap.Tap} instance for use internally. */
public class TempHfs extends Hfs
  {
  /** Field name */
  final String name;
  /** Field schemeClass */
  private Class<? extends Scheme> schemeClass;
  /** Field temporaryPath */

  /** Class NullScheme is a noop scheme used as a placeholder */
  private static class NullScheme extends Scheme<Configuration, RecordReader, OutputCollector, Object, Object>
    {
    @Override
    public void sourceConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf )
      {
      // do nothing
      }

    @Override
    public void sinkConfInit( FlowProcess<? extends Configuration> flowProcess, Tap<Configuration, RecordReader, OutputCollector> tap, Configuration conf )
      {
      conf.setClass( "mapred.output.key.class", Tuple.class, Object.class );
      conf.setClass( "mapred.output.value.class", Tuple.class, Object.class );
      conf.setClass( "mapred.output.format.class", NullOutputFormat.class, OutputFormat.class );
      }

    @Override
    public boolean source( FlowProcess<? extends Configuration> flowProcess, SourceCall<Object, RecordReader> sourceCall ) throws IOException
      {
      return false;
      }

    @Override
    public void sink( FlowProcess<? extends Configuration> flowProcess, SinkCall<Object, OutputCollector> sinkCall ) throws IOException
      {
      }
    }

  /**
   * Constructor TempHfs creates a new TempHfs instance.
   *
   * @param name   of type String
   * @param isNull of type boolean
   */
  public TempHfs( Configuration conf, String name, boolean isNull )
    {
    super( isNull ? new NullScheme() : new SequenceFile()
    {
    } );
    this.name = name;
    this.stringPath = initTemporaryPath( conf, true );
    }

  /**
   * Constructor TempDfs creates a new TempDfs instance.
   *
   * @param name of type String
   */
  public TempHfs( Configuration conf, String name, Class<? extends Scheme> schemeClass )
    {
    this( conf, name, schemeClass, true );
    }

  public TempHfs( Configuration conf, String name, Class<? extends Scheme> schemeClass, boolean unique )
    {
    this.name = name;

    if( schemeClass == null )
      this.schemeClass = SequenceFile.class;
    else
      this.schemeClass = schemeClass;

    this.stringPath = initTemporaryPath( conf, unique );
    }

  public Class<? extends Scheme> getSchemeClass()
    {
    return schemeClass;
    }

  private String initTemporaryPath( Configuration conf, boolean unique )
    {
    String child = unique ? makeTemporaryPathDirString( name ) : name;

    return new Path( getTempPath( conf ), child ).toString();
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    Fields fields = incomingScopes.iterator().next().getIncomingTapFields();

    setSchemeUsing( fields );

    return new Scope( fields );
    }

  private void setSchemeUsing( Fields fields )
    {
    try
      {
      setScheme( schemeClass.getConstructor( Fields.class ).newInstance( fields ) );
      }
    catch( Exception exception )
      {
      throw new CascadingException( "unable to create specified scheme: " + schemeClass.getName(), exception );
      }
    }

  @Override
  public boolean isTemporary()
    {
    return true;
    }

  @Override
  public String toString()
    {
    return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[" + name + "]";
    }
  }
