/*
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

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.Set;

import cascading.flow.Flow;
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
  @ConstructorProperties({"metaInfo", "original"})
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
  @ConstructorProperties({"original"})
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

  @Override
  public void flowConfInit( Flow<Config> flow )
    {
    original.flowConfInit( flow );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    original.sourceConfInit( flowProcess, conf );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Config> flowProcess, Config conf )
    {
    original.sinkConfInit( flowProcess, conf );
    }

  @Override
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

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess, Input input ) throws IOException
    {
    return original.openForRead( flowProcess, input );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.openForRead( flowProcess );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess, Output output ) throws IOException
    {
    return original.openForWrite( flowProcess, output );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.openForWrite( flowProcess );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    return original.outgoingScopeFor( incomingScopes );
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<? extends Config> flowProcess )
    {
    return original.retrieveSourceFields( flowProcess );
    }

  @Override
  public void presentSourceFields( FlowProcess<? extends Config> flowProcess, Fields fields )
    {
    original.presentSourceFields( flowProcess, fields );
    }

  @Override
  public Fields retrieveSinkFields( FlowProcess<? extends Config> flowProcess )
    {
    return original.retrieveSinkFields( flowProcess );
    }

  @Override
  public void presentSinkFields( FlowProcess<? extends Config> flowProcess, Fields fields )
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

  @Override
  public String getFullIdentifier( FlowProcess<? extends Config> flowProcess )
    {
    return original.getFullIdentifier( flowProcess );
    }

  @Override
  public String getFullIdentifier( Config conf )
    {
    return original.getFullIdentifier( conf );
    }

  @Override
  public boolean createResource( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.createResource( flowProcess );
    }

  @Override
  public boolean createResource( Config conf ) throws IOException
    {
    return original.createResource( conf );
    }

  @Override
  public boolean deleteResource( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.deleteResource( flowProcess );
    }

  @Override
  public boolean deleteResource( Config conf ) throws IOException
    {
    return original.deleteResource( conf );
    }

  @Override
  public boolean prepareResourceForRead( Config conf ) throws IOException
    {
    return original.prepareResourceForRead( conf );
    }

  @Override
  public boolean prepareResourceForWrite( Config conf ) throws IOException
    {
    return original.prepareResourceForWrite( conf );
    }

  @Override
  public boolean commitResource( Config conf ) throws IOException
    {
    return original.commitResource( conf );
    }

  @Override
  public boolean rollbackResource( Config conf ) throws IOException
    {
    return original.rollbackResource( conf );
    }

  @Override
  public boolean resourceExists( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.resourceExists( flowProcess );
    }

  @Override
  public boolean resourceExists( Config conf ) throws IOException
    {
    return original.resourceExists( conf );
    }

  @Override
  public long getModifiedTime( FlowProcess<? extends Config> flowProcess ) throws IOException
    {
    return original.getModifiedTime( flowProcess );
    }

  @Override
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
  public ConfigDef getNodeConfigDef()
    {
    return original.getNodeConfigDef();
    }

  @Override
  public boolean hasNodeConfigDef()
    {
    return original.hasNodeConfigDef();
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
  public String toString()
    {
    return getClass().getSimpleName() + "<" + original.toString() + ">";
    }
  }
