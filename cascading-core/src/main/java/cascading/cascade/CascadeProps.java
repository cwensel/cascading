/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.util.Map;
import java.util.Properties;

import cascading.property.Props;

/**
 * Class CascadeProps is a fluent helper class for setting various {@link Cascade} level properties passed
 * through a {@link CascadeConnector}.
 */
public class CascadeProps extends Props
  {
  public static final String MAX_CONCURRENT_FLOWS = "cascading.cascade.maxconcurrentflows";

  int maxConcurrentFlows = 0;

  /**
   * Method setMaxConcurrentFlows sets the maximum number of Flows that a Cascade can run concurrently.
   * <p/>
   * A value of one (1) will run one Flow at a time. A value of zero (0), the default, disables the restriction.
   * <p/>
   * By default a Cascade will attempt to run all give Flow instances at the same time. But there are occasions
   * where limiting the number for flows helps manages resources.
   *
   * @param properties         of type Map<Object, Object>
   * @param numConcurrentFlows of type int
   */
  public static void setMaxConcurrentFlows( Map<Object, Object> properties, int numConcurrentFlows )
    {
    properties.put( MAX_CONCURRENT_FLOWS, Integer.toString( numConcurrentFlows ) );
    }

  public static CascadeProps cascadeProps()
    {
    return new CascadeProps();
    }

  public CascadeProps()
    {
    }

  public int getMaxConcurrentFlows()
    {
    return maxConcurrentFlows;
    }

  public CascadeProps setMaxConcurrentFlows( int maxConcurrentFlows )
    {
    this.maxConcurrentFlows = maxConcurrentFlows;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setMaxConcurrentFlows( properties, maxConcurrentFlows );
    }
  }
