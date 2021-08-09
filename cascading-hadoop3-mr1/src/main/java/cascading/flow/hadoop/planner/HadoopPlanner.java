/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.hadoop.planner;

import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowConnector;
import cascading.flow.FlowConnectorProps;
import cascading.flow.FlowDef;
import cascading.flow.FlowStep;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.HadoopFlowStep;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.BaseFlowStepFactory;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.PlannerInfo;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.process.FlowNodeGraph;
import cascading.flow.planner.process.FlowStepFactory;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.transformer.IntermediateTapElementFactory;
import cascading.property.AppProps;
import cascading.property.PropertyUtil;
import cascading.tap.Tap;
import cascading.tap.hadoop.DistCacheTap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class HadoopPlanner is the core Hadoop MapReduce planner used by default through a {@link cascading.flow.FlowConnector}
 * sub-class.
 * <p>
 * Notes:
 * <p>
 * <strong>Custom JobConf properties</strong><br>
 * A custom JobConf instance can be passed to this planner by calling {@link #copyJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link cascading.flow.FlowConnector} sub-class.
 * <p>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
 */
public class HadoopPlanner extends FlowPlanner<HadoopFlow, JobConf>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopPlanner.class );

  public static final String PLATFORM_NAME = "hadoop";

  /** Field jobConf */
  private JobConf defaultJobConf;
  /** Field intermediateSchemeClass */
  private Class intermediateSchemeClass;

  /**
   * Method copyJobConf adds the given JobConf values to the given properties object. Use this method to pass
   * custom default Hadoop JobConf properties to Hadoop.
   *
   * @param properties of type Map
   * @param jobConf    of type JobConf
   */
  public static void copyJobConf( Map<Object, Object> properties, JobConf jobConf )
    {
    for( Map.Entry<String, String> entry : jobConf )
      properties.put( entry.getKey(), entry.getValue() );
    }

  /**
   * Method createJobConf returns a new JobConf instance using the values in the given properties argument.
   *
   * @param properties of type Map
   * @return a JobConf instance
   */
  public static JobConf createJobConf( Map<Object, Object> properties )
    {
    JobConf conf = new JobConf();

    copyProperties( conf, properties );

    return conf;
    }

  /**
   * Method copyProperties adds the given Map values to the given JobConf object.
   *
   * @param jobConf    of type JobConf
   * @param properties of type Map
   */
  public static void copyProperties( JobConf jobConf, Map<Object, Object> properties )
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
  public JobConf getDefaultConfig()
    {
    return defaultJobConf;
    }

  @Override
  public PlatformInfo getPlatformInfo()
    {
    return HadoopUtil.getPlatformInfo( JobConf.class, "org/apache/hadoop", "Hadoop MR" );
    }

  @Override
  public void initialize( FlowConnector flowConnector, Map<Object, Object> properties )
    {
    super.initialize( flowConnector, properties );

    defaultJobConf = HadoopUtil.createJobConf( properties, createJobConf( properties ) );
    checkPlatform( defaultJobConf );
    intermediateSchemeClass = flowConnector.getIntermediateSchemeClass( properties );

    Class type = AppProps.getApplicationJarClass( properties );
    if( defaultJobConf.getJar() == null && type != null )
      defaultJobConf.setJarByClass( type );

    String path = AppProps.getApplicationJarPath( properties );
    if( defaultJobConf.getJar() == null && path != null )
      defaultJobConf.setJar( path );

    if( defaultJobConf.getJar() == null )
      defaultJobConf.setJarByClass( HadoopUtil.findMainClass( HadoopPlanner.class ) );

    AppProps.setApplicationJarPath( properties, defaultJobConf.getJar() );

    LOG.info( "using application jar: {}", defaultJobConf.getJar() );
    }

  @Override
  public void configRuleRegistryDefaults( RuleRegistry ruleRegistry )
    {
    super.configRuleRegistryDefaults( ruleRegistry );

    ruleRegistry.addDefaultElementFactory( IntermediateTapElementFactory.TEMP_TAP, new TempTapElementFactory() );

    if( PropertyUtil.getBooleanProperty( getDefaultProperties(), FlowConnectorProps.ENABLE_DECORATE_ACCUMULATED_TAP, true ) )
      ruleRegistry.addDefaultElementFactory( IntermediateTapElementFactory.ACCUMULATED_TAP, new TempTapElementFactory( DistCacheTap.class.getName() ) );
    }

  protected void checkPlatform( Configuration conf )
    {
    if( HadoopUtil.isYARN( conf ) )
      LOG.warn( "running YARN based flows on Hadoop 1.x may cause problems, please use the 'cascading-hadoop3-mr1' dependencies" );
    }

  @Override
  protected HadoopFlow createFlow( FlowDef flowDef )
    {
    return new HadoopFlow( getPlatformInfo(), getDefaultProperties(), getDefaultConfig(), flowDef );
    }

  @Override
  public FlowStepFactory<JobConf> getFlowStepFactory()
    {
    return new BaseFlowStepFactory<JobConf>( getFlowNodeFactory() )
      {
      @Override
      public FlowStep<JobConf> createFlowStep( ElementGraph stepElementGraph, FlowNodeGraph flowNodeGraph )
        {
        return new HadoopFlowStep( stepElementGraph, flowNodeGraph );
        }
      };
    }

  public URI getDefaultURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getDefaultFileSystemURIScheme( defaultJobConf );
    }

  public URI getURIScheme( Tap tap )
    {
    return ( (Hfs) tap ).getURIScheme( defaultJobConf );
    }

  @Override
  protected Tap makeTempTap( String prefix, String name )
    {
    // must give Taps unique names
    return new TempHfs( defaultJobConf, Util.makePath( prefix, name ), intermediateSchemeClass, prefix == null );
    }
  }
