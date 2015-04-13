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

package cascading.flow.tez.planner;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowElement;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.transformer.BoundaryElementFactory;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.flow.tez.Hadoop2TezFlow;
import cascading.flow.tez.Hadoop2TezFlowStep;
import cascading.flow.tez.util.TezUtil;
import cascading.pipe.Boundary;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.flow.tez.util.TezUtil.asJobConf;

/**
 */
public class Hadoop2TezPlanner extends FlowPlanner<Hadoop2TezFlow, TezConfiguration>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( Hadoop2TezPlanner.class );

  public static final String PLATFORM_NAME = "hadoop2-tez";

  /** Field defaultConfiguration */
  private TezConfiguration defaultConfiguration;
  /** Field intermediateSchemeClass */
  private Class intermediateSchemeClass;

  public static void copyConfiguration( Map<Object, Object> properties, Configuration configuration )
    {
    for( Map.Entry<String, String> entry : configuration )
      properties.put( entry.getKey(), entry.getValue() );
    }

  public static TezConfiguration createConfiguration( Map<Object, Object> properties )
    {
    TezConfiguration conf = new TezConfiguration();

    copyProperties( conf, properties );

    return conf;
    }

  public static void copyProperties( Configuration jobConf, Map<Object, Object> properties )
    {
    if( properties instanceof Properties )
      {
      Properties props = (Properties) properties;
      Set<String> keys = props.stringPropertyNames();

      for( String key : keys )
        jobConf.set( key, props.getProperty( key ) );
      }
    else
      {
      for( Map.Entry<Object, Object> entry : properties.entrySet() )
        {
        if( entry.getValue() != null )
          jobConf.set( entry.getKey().toString(), entry.getValue().toString() );
        }
      }
    }

  @Override
  public PlannerInfo getPlannerInfo( String registryName )
    {
    return new PlannerInfo( getClass().getSimpleName(), PLATFORM_NAME, registryName );
    }

  @Override
  public TezConfiguration getDefaultConfig()
    {
    return defaultConfiguration;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return HadoopUtil.getPlatformInfo();
    }

  @Override
  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    super.initialize( flowConnector, properties );

    defaultConfiguration = TezUtil.createTezConf( properties, createConfiguration( properties ) );
    intermediateSchemeClass = flowConnector.getIntermediateSchemeClass( properties );

    String applicationJarPath = AppProps.getApplicationJarPath( properties );

    if( applicationJarPath == null )
      {
      Class type = AppProps.getApplicationJarClass( properties );

      if( type == null )
        type = HadoopUtil.findMainClass( Hadoop2TezPlanner.class );

      if( type != null )
        applicationJarPath = Util.findContainingJar( type );

      AppProps.setApplicationJarPath( properties, applicationJarPath );
      }

    if( applicationJarPath != null )
      LOG.info( "using application jar: {}", applicationJarPath );
    else
      LOG.info( "using application jar not provided, see cascading.property.AppProps for more information" );
    }

  @Override
  public void configRuleRegistryDefaults( RuleRegistry ruleRegistry )
    {
    super.configRuleRegistryDefaults( ruleRegistry );

    ruleRegistry.addDefaultElementFactory( IntermediateTapElementFactory.TEMP_TAP, new TempTapElementFactory() );
    ruleRegistry.addDefaultElementFactory( BoundaryElementFactory.BOUNDARY_PIPE, new IntermediateBoundaryElementFactory() );
    }

  @Override
  protected Hadoop2TezFlow createFlow( FlowDef flowDef )
    {
    return new Hadoop2TezFlow( getPlatformInfo(), getDefaultProperties(), getDefaultConfig(), flowDef );
    }

  public FlowStep<TezConfiguration> createFlowStep( ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph )
    {
    return new Hadoop2TezFlowStep( stepElementGraph, flowNodeGraph );
    }

  public URI getDefaultURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getDefaultFileSystemURIScheme( defaultConfiguration );
    }

  public URI getURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getURIScheme( defaultConfiguration );
    }

  @Override
  protected Tap makeTempTap( String prefix, String name )
    {
    // must give Taps unique names
    return new TempHfs( asJobConf( defaultConfiguration ), Util.makePath( prefix, name ), intermediateSchemeClass, prefix == null );
    }

  public class IntermediateBoundaryElementFactory extends BoundaryElementFactory
    {

    @Override
    public FlowElement create( ElementGraph graph, FlowElement flowElement )
      {
      return new Boundary();
      }
    }
  }
