/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.util;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * ChildFirstURLClassLoader is an internal utility class used to load CascadingServices from an isolated
 * classpath to prevent collisions between dependent jar versions in the parent classpath.
 */
class ChildFirstURLClassLoader extends ClassLoader
  {
  private ChildURLClassLoader childClassLoader;

  private static class ChildURLClassLoader extends URLClassLoader
    {
    private ClassLoader parentClassLoader;

    public ChildURLClassLoader( URL[] urls, ClassLoader parentClassLoader )
      {
      super( urls, null );

      this.parentClassLoader = parentClassLoader;
      }

    @Override
    public Class<?> findClass( String name ) throws ClassNotFoundException
      {
      if( name.startsWith( "cascading." ) )
        return parentClassLoader.loadClass( name );

      try
        {
        return super.findClass( name );
        }
      catch( ClassNotFoundException exception )
        {
        return parentClassLoader.loadClass( name );
        }
      }
    }

  public ChildFirstURLClassLoader( URL... urls )
    {
    super( Thread.currentThread().getContextClassLoader() );

    childClassLoader = new ChildURLClassLoader( urls, this.getParent() );
    }

  @Override
  protected synchronized Class<?> loadClass( String name, boolean resolve ) throws ClassNotFoundException
    {
    if( name.startsWith( "cascading." ) )
      return super.loadClass( name, resolve );

    try
      {
      return childClassLoader.loadClass( name );
      }
    catch( ClassNotFoundException exception )
      {
      return super.loadClass( name, resolve );
      }
    }
  }
