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

package cascading.cascade;

import java.beans.ConstructorProperties;
import java.util.Collection;
import java.util.Map;

import cascading.cascade.planner.FlowGraph;
import cascading.cascade.planner.IdentifierGraph;
import cascading.flow.Flow;
import cascading.tap.Tap;
import cascading.util.Util;

import static cascading.cascade.CascadeDef.cascadeDef;

/**
 * Class CascadeConnector is used to construct a new {@link Cascade} instance from a collection of {@link cascading.flow.Flow} instance.
 * <p/>
 * Note order is not significant when adding passing Flow instances to the {@code connect}
 * method. This connector will order them based on their dependencies, if any.
 * <p/>
 * One Flow is dependent on another if the first sinks (produces) output that the second Flow sources (consumes) as
 * input. A sink and source are considered equivalent if the fully qualified identifier, typically {@link Tap#getFullIdentifier(Object)}
 * from either are {@code equals()}.
 * <p/>
 * <p/>
 * Note that checkpoint sink Taps from an upstream Flow may be the sources to downstream Flow instances.
 * <p/>
 * The {@link CascadeDef} is a convenience class for dynamically defining a Cascade that can be passed to the
 * {@link CascadeConnector#connect(CascadeDef)} method.
 * <p/>
 * Use the {@link CascadeProps} fluent helper class to create global properties to pass to the CascadeConnector
 * constructor.
 *
 * @see CascadeDef
 * @see CascadeProps
 */
public class CascadeConnector
  {
  /** Field properties */
  private Map<Object, Object> properties;

  /** Constructor CascadeConnector creates a new CascadeConnector instance. */
  public CascadeConnector()
    {
    }

  /**
   * Constructor CascadeConnector creates a new CascadeConnector instance.
   *
   * @param properties of type Map<Object, Object>
   */
  @ConstructorProperties({"properties"})
  public CascadeConnector( Map<Object, Object> properties )
    {
    this.properties = properties;
    }

  /**
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
   * of the Cascade is derived from the given Flow instances.
   *
   * @param flows of type Collection<Flow>
   * @return Cascade
   */
  public Cascade connect( Collection<Flow> flows )
    {
    return connect( null, flows.toArray( new Flow[ flows.size() ] ) );
    }

  /**
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance.
   *
   * @param name  of type String
   * @param flows of type Collection<Flow>
   * @return Cascade
   */
  public Cascade connect( String name, Collection<Flow> flows )
    {
    return connect( name, flows.toArray( new Flow[ flows.size() ] ) );
    }

  /**
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance. The name
   * of the Cascade is derived from the given Flow instances.
   *
   * @param flows of type Flow
   * @return Cascade
   */
  public Cascade connect( Flow... flows )
    {
    return connect( null, flows );
    }

  /**
   * Given any number of {@link cascading.flow.Flow} objects, it will connect them and return a new {@link Cascade} instance.
   *
   * @param name  of type String
   * @param flows of type Flow
   * @return Cascade
   */
  public Cascade connect( String name, Flow... flows )
    {
    name = name == null ? makeName( flows ) : name;

    CascadeDef cascadeDef = cascadeDef()
      .setName( name )
      .addFlows( flows );

    return connect( cascadeDef );
    }

  public Cascade connect( CascadeDef cascadeDef )
    {
    IdentifierGraph identifierGraph = new IdentifierGraph( cascadeDef.getFlowsArray() );
    FlowGraph flowGraph = new FlowGraph( identifierGraph );

    return new BaseCascade( cascadeDef, properties, flowGraph, identifierGraph );
    }

  private String makeName( Flow[] flows )
    {
    String[] names = new String[ flows.length ];

    for( int i = 0; i < flows.length; i++ )
      names[ i ] = flows[ i ].getName();

    return Util.join( names, "+" );
    }
  }
