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

package cascading.flow;

import java.util.Map;
import java.util.Properties;

import cascading.operation.AssertionLevel;
import cascading.operation.DebugLevel;
import cascading.property.Props;
import cascading.scheme.Scheme;

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

  AssertionLevel assertionLevel;
  DebugLevel debugLevel;
  String intermediateSchemeClassName;

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
    properties.put( INTERMEDIATE_SCHEME_CLASS, intermediateSchemeClass );
    }

  /**
   * Method setIntermediateSchemeClass is used for debugging.
   *
   * @param properties              of type Map<Object, Object>
   * @param intermediateSchemeClass of type String
   */
  public static void setIntermediateSchemeClass( Map<Object, Object> properties, String intermediateSchemeClass )
    {
    properties.put( INTERMEDIATE_SCHEME_CLASS, intermediateSchemeClass );
    }

  public FlowConnectorProps()
    {
    }

  public AssertionLevel getAssertionLevel()
    {
    return assertionLevel;
    }

  public FlowConnectorProps setAssertionLevel( AssertionLevel assertionLevel )
    {
    this.assertionLevel = assertionLevel;

    return this;
    }

  public DebugLevel getDebugLevel()
    {
    return debugLevel;
    }

  public FlowConnectorProps setDebugLevel( DebugLevel debugLevel )
    {
    this.debugLevel = debugLevel;

    return this;
    }

  public String getIntermediateSchemeClassName()
    {
    return intermediateSchemeClassName;
    }

  public FlowConnectorProps setIntermediateSchemeClassName( String intermediateSchemeClassName )
    {
    this.intermediateSchemeClassName = intermediateSchemeClassName;

    return this;
    }

  public FlowConnectorProps setIntermediateSchemeClassName( Class<Scheme> intermediateSchemeClass )
    {
    if( intermediateSchemeClass != null )
      this.intermediateSchemeClassName = intermediateSchemeClass.getName();

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setAssertionLevel( properties, assertionLevel );
    setDebugLevel( properties, debugLevel );
    setIntermediateSchemeClass( properties, intermediateSchemeClassName );
    }

  }
