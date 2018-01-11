/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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
import java.util.function.Function;

import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.property.ConfigDef;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;

/**
 * Class AdaptorTap wraps a given {@link Tap} instance, delegating all calls to the original. In addition, AdaptorTap
 * implementations must provide {@link Function} implementations that will adapt one Config and {@link FlowProcess}
 * type from the current platform types to the underlying Tap platform types.
 */
public class AdaptorTap<TConfig, TInput, TOutput, OConfig, OInput, OOutput> extends Tap<TConfig, TInput, TOutput>
  {
  protected Tap<OConfig, OInput, OOutput> original;
  protected Function<FlowProcess<? extends TConfig>, FlowProcess<? extends OConfig>> processProvider;
  protected Function<TConfig, OConfig> configProvider;

  @ConstructorProperties({"original", "processProvider", "configProvider"})
  public AdaptorTap( Tap<OConfig, OInput, OOutput> original, Function<FlowProcess<? extends TConfig>, FlowProcess<? extends OConfig>> processProvider, Function<TConfig, OConfig> configProvider )
    {
    setOriginal( original );

    if( processProvider == null )
      throw new IllegalArgumentException( "processProvider may not be null" );

    if( configProvider == null )
      throw new IllegalArgumentException( "confProvider may not be null" );

    this.processProvider = processProvider;
    this.configProvider = configProvider;
    }

  public Tap<OConfig, OInput, OOutput> getOriginal()
    {
    return original;
    }

  protected void setOriginal( Tap<OConfig, OInput, OOutput> original )
    {
    if( original == null )
      throw new IllegalArgumentException( "wrapped tap value may not be null" );

    this.original = original;
    }

  @Override
  public Scheme<TConfig, TInput, TOutput, ?, ?> getScheme()
    {
    throw new UnsupportedOperationException( "cannot retrieve Scheme" );
    }

  @Override
  public String getTrace()
    {
    return original.getTrace();
    }

  @Override
  public void flowConfInit( Flow<TConfig> flow )
    {
    // do nothing
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends TConfig> flowProcess, TConfig conf )
    {
    original.sourceConfInit( processProvider.apply( flowProcess ), configProvider.apply( conf ) );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends TConfig> flowProcess, TConfig conf )
    {
    original.sinkConfInit( processProvider.apply( flowProcess ), configProvider.apply( conf ) );
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
  public TupleEntryIterator openForRead( FlowProcess<? extends TConfig> flowProcess, TInput input ) throws IOException
    {
    return original.openForRead( processProvider.apply( flowProcess ), null );
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.openForRead( processProvider.apply( flowProcess ) );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends TConfig> flowProcess, TOutput output ) throws IOException
    {
    return original.openForWrite( processProvider.apply( flowProcess ), null );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.openForWrite( processProvider.apply( flowProcess ) );
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    return original.outgoingScopeFor( incomingScopes );
    }

  @Override
  public Fields retrieveSourceFields( FlowProcess<? extends TConfig> flowProcess )
    {
    return original.retrieveSourceFields( processProvider.apply( flowProcess ) );
    }

  @Override
  public void presentSourceFields( FlowProcess<? extends TConfig> flowProcess, Fields fields )
    {
    original.presentSourceFields( processProvider.apply( flowProcess ), fields );
    }

  @Override
  public Fields retrieveSinkFields( FlowProcess<? extends TConfig> flowProcess )
    {
    return original.retrieveSinkFields( processProvider.apply( flowProcess ) );
    }

  @Override
  public void presentSinkFields( FlowProcess<? extends TConfig> flowProcess, Fields fields )
    {
    original.presentSinkFields( processProvider.apply( flowProcess ), fields );
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
  public String getFullIdentifier( FlowProcess<? extends TConfig> flowProcess )
    {
    return original.getFullIdentifier( processProvider.apply( flowProcess ) );
    }

  @Override
  public String getFullIdentifier( TConfig conf )
    {
    return original.getFullIdentifier( configProvider.apply( conf ) );
    }

  @Override
  public boolean createResource( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.createResource( processProvider.apply( flowProcess ) );
    }

  @Override
  public boolean createResource( TConfig conf ) throws IOException
    {
    return original.createResource( configProvider.apply( conf ) );
    }

  @Override
  public boolean deleteResource( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.deleteResource( processProvider.apply( flowProcess ) );
    }

  @Override
  public boolean deleteResource( TConfig conf ) throws IOException
    {
    return original.deleteResource( configProvider.apply( conf ) );
    }

  @Override
  public boolean prepareResourceForRead( TConfig conf ) throws IOException
    {
    return original.prepareResourceForRead( configProvider.apply( conf ) );
    }

  @Override
  public boolean prepareResourceForWrite( TConfig conf ) throws IOException
    {
    return original.prepareResourceForWrite( configProvider.apply( conf ) );
    }

  @Override
  public boolean commitResource( TConfig conf ) throws IOException
    {
    return original.commitResource( configProvider.apply( conf ) );
    }

  @Override
  public boolean rollbackResource( TConfig conf ) throws IOException
    {
    return original.rollbackResource( configProvider.apply( conf ) );
    }

  @Override
  public boolean resourceExists( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.resourceExists( processProvider.apply( flowProcess ) );
    }

  @Override
  public boolean resourceExists( TConfig conf ) throws IOException
    {
    return original.resourceExists( configProvider.apply( conf ) );
    }

  @Override
  public long getModifiedTime( FlowProcess<? extends TConfig> flowProcess ) throws IOException
    {
    return original.getModifiedTime( processProvider.apply( flowProcess ) );
    }

  @Override
  public long getModifiedTime( TConfig conf ) throws IOException
    {
    return original.getModifiedTime( configProvider.apply( conf ) );
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
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;

    Tap tap = (Tap) object;

    if( tap instanceof AdaptorTap )
      tap = ( (AdaptorTap) tap ).getOriginal();

    if( getIdentifier() != null ? !getIdentifier().equals( tap.getIdentifier() ) : tap.getIdentifier() != null )
      return false;

    if( original.getScheme() != null ? !original.getScheme().equals( tap.getScheme() ) : tap.getScheme() != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = getIdentifier() != null ? getIdentifier().hashCode() : 0;

    result = 31 * result + ( original.getScheme() != null ? original.getScheme().hashCode() : 0 );

    return result;
    }

  @Override
  public String toString()
    {
    if( getIdentifier() != null )
      return getClass().getSimpleName() + "[\"" + original.getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( getIdentifier() ) + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + original.getScheme() + "\"]" + "[not initialized]";
    }
  }
