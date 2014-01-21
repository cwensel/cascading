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

package cascading.flow.hadoop.planner;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlow;
import cascading.flow.hadoop.util.HadoopUtil;
import cascading.flow.planner.FlowElementGraph;
import cascading.flow.planner.FlowPlanner;
import cascading.flow.planner.FlowStepGraph;
import cascading.flow.planner.PlatformInfo;
import cascading.flow.planner.graph.ElementGraph;
import cascading.flow.planner.rule.RuleRegistry;
import cascading.flow.planner.rule.transformer.RuleTempTapInsertionTransformer;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.util.TempHfs;
import cascading.util.Util;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class HadoopPlanner is the core Hadoop MapReduce planner used by default through the {@link cascading.flow.hadoop.HadoopFlowConnector}.
 * <p/>
 * Notes:
 * <p/>
 * <strong>Custom JobConf properties</strong><br/>
 * A custom JobConf instance can be passed to this planner by calling {@link #copyJobConf(java.util.Map, org.apache.hadoop.mapred.JobConf)}
 * on a map properties object before constructing a new {@link cascading.flow.hadoop.HadoopFlowConnector}.
 * <p/>
 * A better practice would be to set Hadoop properties directly on the map properties object handed to the FlowConnector.
 * All values in the map will be passed to a new default JobConf instance to be used as defaults for all resulting
 * Flow instances.
 * <p/>
 * For example, {@code properties.set("mapred.child.java.opts","-Xmx512m");} would convince Hadoop
 * to spawn all child jvms with a heap of 512MB.
 */
public class HadoopPlanner extends FlowPlanner<HadoopFlow, JobConf>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( HadoopPlanner.class );

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
  public JobConf getDefaultConfig()
    {
    return defaultJobConf;
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

    defaultJobConf = HadoopUtil.createJobConf( properties, createJobConf( properties ) );
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
  protected FlowStepGraph<JobConf> createStepGraph( FlowDef flowDef, FlowElementGraph flowElementGraph, List<ElementGraph> elementSubGraphs )
    {
    return new HadoopStepGraph( flowElementGraph, elementSubGraphs );
    }

  @Override
  protected RuleRegistry getRuleRegistry( FlowDef flowDef )
    {
    return new HadoopRuleRegistry();
    }

  @Override
  protected void configRuleRegistry( RuleRegistry ruleRegistry )
    {
    super.configRuleRegistry( ruleRegistry );

    ruleRegistry.addElementFactory( RuleTempTapInsertionTransformer.TEMP_TAP, new TempTapElementFactory() );
    }

  @Override
  protected HadoopFlow createFlow( FlowDef flowDef )
    {
    return new HadoopFlow( getPlatformInfo(), getDefaultProperties(), getDefaultConfig(), flowDef );
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
