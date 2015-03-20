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

package cascading.flow;

import java.util.Map;
import java.util.Properties;

import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.property.Props;
import cascading.scheme.Scheme;
import cascading.tap.DecoratorTap;

/**
 * The class FlowConnectorProps is a fluent helper class for setting {@link FlowConnector} specific
 * properties through the {@link FlowConnector} constructor.
 *
 * @see cascading.property.AppProps
 * @see cascading.cascade.CascadeProps
 * @see FlowProps
 */
public class FlowConnectorProps extends Props
  {
  public static final String ASSERTION_LEVEL = "cascading.flowconnector.assertionlevel";
  public static final String DEBUG_LEVEL = "cascading.flowconnector.debuglevel";
  public static final String INTERMEDIATE_SCHEME_CLASS = "cascading.flowconnector.intermediateschemeclass";
  public static final String TEMPORARY_TAP_DECORATOR_CLASS = "cascading.flowconnector.temporary_tap.decorator.classname";
  public static final String CHECKPOINT_TAP_DECORATOR_CLASS = "cascading.flowconnector.checkpoint_tap.decorator.classname";

  AssertionLevel assertionLevel;
  DebugLevel debugLevel;
  String intermediateSchemeClassName;
  String temporaryTapDecoratorClassName;
  String checkpointTapDecoratorClassName;

  /**
   * Method setAssertionLevel sets the target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param properties     of type Map<Object, Object>
   * @param assertionLevel of type AssertionLevel
   */
  public static void setAssertionLevel( Map<Object, Object> properties, AssertionLevel assertionLevel )
    {
    if( assertionLevel != null )
      properties.put( ASSERTION_LEVEL, assertionLevel.toString() );
    }

  /**
   * Method setDebugLevel sets the target planner {@link cascading.operation.DebugLevel}.
   *
   * @param properties of type Map<Object, Object>
   * @param debugLevel of type DebugLevel
   */
  public static void setDebugLevel( Map<Object, Object> properties, DebugLevel debugLevel )
    {
    if( debugLevel != null )
      properties.put( DEBUG_LEVEL, debugLevel.toString() );
    }

  /**
   * Method setIntermediateSchemeClass is used for debugging.
   *
   * @param properties              of type Map<Object, Object>
   * @param intermediateSchemeClass of type Class
   */
  public static void setIntermediateSchemeClass( Map<Object, Object> properties, Class<? extends Scheme> intermediateSchemeClass )
    {
    if( intermediateSchemeClass != null )
      properties.put( INTERMEDIATE_SCHEME_CLASS, intermediateSchemeClass.getName() );
    }

  /**
   * Method setIntermediateSchemeClass is used for debugging.
   *
   * @param properties                  of type Map<Object, Object>
   * @param intermediateSchemeClassName of type String
   */
  public static void setIntermediateSchemeClass( Map<Object, Object> properties, String intermediateSchemeClassName )
    {
    if( intermediateSchemeClassName != null )
      properties.put( INTERMEDIATE_SCHEME_CLASS, intermediateSchemeClassName );
    }

  /**
   * Method temporaryTapDecoratorClassName is used for wrapping a intermediate temporary Tap.
   *
   * @param properties                     of type Map<Object, Object>
   * @param temporaryTapDecoratorClassName of type String
   */
  public static void setTemporaryTapDecoratorClass( Map<Object, Object> properties, String temporaryTapDecoratorClassName )
    {
    if( temporaryTapDecoratorClassName != null )
      properties.put( TEMPORARY_TAP_DECORATOR_CLASS, temporaryTapDecoratorClassName );
    }

  /**
   * Method checkpointTapDecoratorClassName is used for wrapping a checkpoint Tap.
   *
   * @param properties                      of type Map<Object, Object>
   * @param checkpointTapDecoratorClassName of type String
   */
  public static void setCheckpointTapDecoratorClass( Map<Object, Object> properties, String checkpointTapDecoratorClassName )
    {
    if( checkpointTapDecoratorClassName != null )
      properties.put( CHECKPOINT_TAP_DECORATOR_CLASS, checkpointTapDecoratorClassName );
    }

  /**
   * Creates a new FlowConnectorProps instance.
   *
   * @return FlowConnectorProps instance
   */
  public static FlowConnectorProps flowConnectorProps()
    {
    return new FlowConnectorProps();
    }

  public FlowConnectorProps()
    {
    }

  public AssertionLevel getAssertionLevel()
    {
    return assertionLevel;
    }

  /**
   * Method setAssertionLevel sets the target planner {@link cascading.operation.AssertionLevel}.
   *
   * @param assertionLevel of type AssertionLevel
   * @return this instance
   */
  public FlowConnectorProps setAssertionLevel( AssertionLevel assertionLevel )
    {
    this.assertionLevel = assertionLevel;

    return this;
    }

  public DebugLevel getDebugLevel()
    {
    return debugLevel;
    }

  /**
   * Method setDebugLevel sets the target planner {@link cascading.operation.DebugLevel}.
   *
   * @param debugLevel of type DebugLevel
   * @return this instance
   */
  public FlowConnectorProps setDebugLevel( DebugLevel debugLevel )
    {
    this.debugLevel = debugLevel;

    return this;
    }

  public String getIntermediateSchemeClassName()
    {
    return intermediateSchemeClassName;
    }

  /**
   * Method setIntermediateSchemeClassName is used for debugging.
   *
   * @param intermediateSchemeClassName of type String
   * @return this instance
   */
  public FlowConnectorProps setIntermediateSchemeClassName( String intermediateSchemeClassName )
    {
    this.intermediateSchemeClassName = intermediateSchemeClassName;

    return this;
    }

  /**
   * Method setIntermediateSchemeClassName is used for debugging.
   *
   * @param intermediateSchemeClass of type Class
   * @return this instance
   */
  public FlowConnectorProps setIntermediateSchemeClassName( Class<Scheme> intermediateSchemeClass )
    {
    if( intermediateSchemeClass != null )
      this.intermediateSchemeClassName = intermediateSchemeClass.getName();

    return this;
    }

  public String getTemporaryTapDecoratorClassName()
    {
    return temporaryTapDecoratorClassName;
    }

  /**
   * Method setTemporaryTapDecoratorClassName sets the class of a {@link cascading.tap.DecoratorTap} to use to
   * wrap an intermediate temporary Tap instance internal to the Flow.
   *
   * @param temporaryTapDecoratorClassName of type String
   * @return this instance
   */
  public FlowConnectorProps setTemporaryTapDecoratorClassName( String temporaryTapDecoratorClassName )
    {
    this.temporaryTapDecoratorClassName = temporaryTapDecoratorClassName;

    return this;
    }

  /**
   * Method setTemporaryTapDecoratorClassName sets the class of a {@link cascading.tap.DecoratorTap} to use to
   * wrap an intermediate temporary Tap instance internal to the Flow.
   *
   * @param temporaryTapDecoratorClass of type Class
   * @return this instance
   */
  public FlowConnectorProps setTemporaryTapDecoratorClassName( Class<DecoratorTap> temporaryTapDecoratorClass )
    {
    if( temporaryTapDecoratorClass != null )
      this.temporaryTapDecoratorClassName = temporaryTapDecoratorClass.getName();

    return this;
    }

  public String getCheckpointTapDecoratorClassName()
    {
    return checkpointTapDecoratorClassName;
    }

  /**
   * Method setCheckpointTapDecoratorClassName sets the class of a {@link cascading.tap.DecoratorTap} to use to
   * wrap an Checkpoint Tap instance within the Flow.
   *
   * @param checkpointTapDecoratorClassName of type String
   * @return this instance
   */
  public FlowConnectorProps setCheckpointTapDecoratorClassName( String checkpointTapDecoratorClassName )
    {
    this.checkpointTapDecoratorClassName = checkpointTapDecoratorClassName;

    return this;
    }

  /**
   * Method setCheckpointTapDecoratorClassName sets the class of a {@link cascading.tap.DecoratorTap} to use to
   * wrap an Checkpoint Tap instance within the Flow.
   *
   * @param checkpointTapDecoratorClass of type Class
   * @return this instance
   */
  public FlowConnectorProps setCheckpointTapDecoratorClassName( Class<DecoratorTap> checkpointTapDecoratorClass )
    {
    if( checkpointTapDecoratorClass != null )
      this.checkpointTapDecoratorClassName = checkpointTapDecoratorClass.getName();

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setAssertionLevel( properties, assertionLevel );
    setDebugLevel( properties, debugLevel );
    setIntermediateSchemeClass( properties, intermediateSchemeClassName );
    setTemporaryTapDecoratorClass( properties, temporaryTapDecoratorClassName );
    setCheckpointTapDecoratorClass( properties, checkpointTapDecoratorClassName );
    }
  }
