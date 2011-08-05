/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.util;

import java.util.Map;

import cascading.CascadingException;

/**
 *
 */
public class ServiceUtil
  {

  // look in meta-inf/cascading-services for all classnames

  public static Map<String, String> findAllServices()
    {
    return null;
    }

  public static CascadingService loadServiceFrom( Map<Object, Object> properties, String property )
    {
    return createService( properties, (String) properties.get( property ) );
    }

  public static CascadingService createService( Map<Object, Object> properties, String className )
    {
    if( className == null )
      return null;

    try
      {
      Class type = Thread.currentThread().getContextClassLoader().loadClass( className );

      CascadingService cluster = (CascadingService) type.newInstance();

      cluster.setProperties( properties );

      return cluster;
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadingException( "unable to find cluster class: " + className, exception );
      }
    catch( IllegalAccessException exception )
      {
      throw new CascadingException( "unable to instantiate cluster class: " + className, exception );
      }
    catch( InstantiationException exception )
      {
      throw new CascadingException( "unable to instantiate cluster class: " + className, exception );
      }

    }
  }
