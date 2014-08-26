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

package cascading.tap;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.Set;

import cascading.flow.Flow;
import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.property.ConfigDef;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

/**
 * Class DecoratorTap wraps a given {@link Tap} instance, delegating all calls to the original.
 * <p/>
 * It also provides an additional generic field that may hold any custom type, this allows implementations
 * to attach any meta-info to the tap being decorated.
 * <p/>
 * Further, sub-classes of DecoratorTap can be used to decorate any Tap instances associated or created for
 * use by the {@link cascading.pipe.Checkpoint} {@link cascading.pipe.Pipe}.
 * <p/>
 * Sub-classing this is optional if the aim is to simply attach relevant meta-info for use by a given application.
 * <p/>
 * In order to pass any meta-info to a management service via the {@link cascading.management.annotation.Property}
 * annotation, a sub-class of DecoratorTap must be provided.
 */
public class DecoratorTap<MetaInfo, Config, Input, Output> extends Tap<Config, Input, Output>
  {
  protected MetaInfo metaInfo;
  protected Tap<Config, Input, Output> original;

  /**
   * Creates a new Tap instance, wrapping the given Tap, and associates the given MetaInfo type with
   * the wrapped Tap instance.
   *
   * @param metaInfo meta-information about the current Tap
   * @param original the decorated Tap instance
   */
  @ConstructorProperties( {"metaInfo", "original"} )
  public DecoratorTap( MetaInfo metaInfo, Tap<Config, Input, Output> original )
    {
    setMetaInfo( metaInfo );
    setOriginal( original );
    }

  /**
   * Creates a new Tap instance, wrapping the given Tap, and associates the given MetaInfo type with
   * the wrapped Tap instance.
   *
   * @param original the decorated Tap instance
   */
  @ConstructorProperties( {"original"} )
  public DecoratorTap( Tap<Config, Input, Output> original )
    {
    setOriginal( original );
    }

  public MetaInfo getMetaInfo()
    {
    return metaInfo;
    }

  public Tap<Config, Input, Output> getOriginal()
    {
    return original;
    }

  protected void setOriginal( Tap<Config, Input, Output> original )
    {
    if( original == null )
      throw new IllegalArgumentException( "wrapped tap value may not be null" );

    this.original = original;
    }

  protected void setMetaInfo( MetaInfo metaInfo )
    {
    this.metaInfo = metaInfo;
    }

  @Override
  public Scheme<Config, Input, Output, ?, ?> getScheme()
    {
    return original.getScheme();
    }

  @Override
  public String getTrace()
    {
    return original.getTrace();
    }

  public void flowConfInit( Flow<Config> flow )
    {
    original.flowConfInit( flow );
    }

  public void sourceConfInit( FlowProcess<Config> flowProcess, Config conf )
    {
    original.sourceConfInit( flowProcess, conf );
    }

  public void sinkConfInit( FlowProcess<Config> flowProcess, Config conf )
    {
    original.sinkConfInit( flowProcess, conf );
    }

  public String getIdentifier()
    {
    return original.getIdentifier();
    }

  @Override
  public Fields getSourceFields()
    {
    return original.getSourceFields();
    }

  @Override
  public Fields getSinkFields()
    {
    return original.getSinkFields();
    }

  public TupleEntryIterator openForRead( FlowProcess<Config> flowProcess, Input input ) throws IOException
    {
    return original.openForRead( flowProcess, input );
    }

  public TupleEntryIterator openForRead( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.openForRead( flowProcess );
    }

  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess, Output output ) throws IOException
    {
    return original.openForWrite( flowProcess, output );
    }

  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.openForWrite( flowProcess );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    return original.outgoingScopeFor( incomingScopes );
    }

  public Fields retrieveSourceFields( FlowProcess<Config> flowProcess )
    {
    return original.retrieveSourceFields( flowProcess );
    }

  public void presentSourceFields( FlowProcess<Config> flowProcess, Fields fields )
    {
    original.presentSourceFields( flowProcess, fields );
    }

  public Fields retrieveSinkFields( FlowProcess<Config> flowProcess )
    {
    return original.retrieveSinkFields( flowProcess );
    }

  public void presentSinkFields( FlowProcess<Config> flowProcess, Fields fields )
    {
    original.presentSinkFields( flowProcess, fields );
    }

  @Override
  public Fields resolveIncomingOperationArgumentFields( Scope incomingScope )
    {
    return original.resolveIncomingOperationArgumentFields( incomingScope );
    }

  @Override
  public Fields resolveIncomingOperationPassThroughFields( Scope incomingScope )
    {
    return original.resolveIncomingOperationPassThroughFields( incomingScope );
    }

  public String getFullIdentifier( FlowProcess<Config> flowProcess )
    {
    return original.getFullIdentifier( flowProcess );
    }

  public String getFullIdentifier( Config conf )
    {
    return original.getFullIdentifier( conf );
    }

  public boolean createResource( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.createResource( flowProcess );
    }

  public boolean createResource( Config conf ) throws IOException
    {
    return original.createResource( conf );
    }

  public boolean deleteResource( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.deleteResource( flowProcess );
    }

  public boolean deleteResource( Config conf ) throws IOException
    {
    return original.deleteResource( conf );
    }

  public boolean commitResource( Config conf ) throws IOException
    {
    return original.commitResource( conf );
    }

  public boolean rollbackResource( Config conf ) throws IOException
    {
    return original.rollbackResource( conf );
    }

  public boolean resourceExists( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.resourceExists( flowProcess );
    }

  public boolean resourceExists( Config conf ) throws IOException
    {
    return original.resourceExists( conf );
    }

  public long getModifiedTime( FlowProcess<Config> flowProcess ) throws IOException
    {
    return original.getModifiedTime( flowProcess );
    }

  public long getModifiedTime( Config conf ) throws IOException
    {
    return original.getModifiedTime( conf );
    }

  @Override
  public SinkMode getSinkMode()
    {
    return original.getSinkMode();
    }

  @Override
  public boolean isKeep()
    {
    return original.isKeep();
    }

  @Override
  public boolean isReplace()
    {
    return original.isReplace();
    }

  @Override
  public boolean isUpdate()
    {
    return original.isUpdate();
    }

  @Override
  public boolean isSink()
    {
    return original.isSink();
    }

  @Override
  public boolean isSource()
    {
    return original.isSource();
    }

  @Override
  public boolean isTemporary()
    {
    return original.isTemporary();
    }

  @Override
  public ConfigDef getConfigDef()
    {
    return original.getConfigDef();
    }

  @Override
  public boolean hasConfigDef()
    {
    return original.hasConfigDef();
    }

  @Override
  public ConfigDef getStepConfigDef()
    {
    return original.getStepConfigDef();
    }

  @Override
  public boolean hasStepConfigDef()
    {
    return original.hasStepConfigDef();
    }

  @Override
  public boolean isEquivalentTo( FlowElement element )
    {
    return original.isEquivalentTo( element );
    }

  @Override
  public String toString()
    {
    return original.toString();
    }
  }
