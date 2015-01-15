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

package cascading.provider;

import cascading.flow.FlowProcess;

/**
 * Class FactoryLoader is an implementation of a {@link ProviderLoader} and is used to load
 * {@link CascadingFactory} instances used by internal frameworks.
 *
 * @see CascadingFactory
 */
public class FactoryLoader extends ProviderLoader<CascadingFactory>
  {
  private static FactoryLoader factoryLoader;

  public synchronized static FactoryLoader getInstance()
    {
    if( factoryLoader == null )
      factoryLoader = new FactoryLoader();

    return factoryLoader;
    }

  public <Factory extends CascadingFactory> Factory loadFactoryFrom( FlowProcess flowProcess, String property, Class<Factory> defaultFactory )
    {
    Object value = flowProcess.getProperty( property );

    String className;

    if( value == null )
      className = defaultFactory.getName();
    else
      className = value.toString();

    Factory factory = (Factory) createProvider( className ); // todo remove this cast

    if( factory != null )
      factory.initialize( flowProcess );

    return factory;
    }
  }
