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

package cascading.bind;

import java.util.Collection;
import java.util.Properties;

import cascading.bind.factory.CascadeFactory;
import cascading.bind.factory.ProcessFactory;
import cascading.cascade.Cascade;

/** A mock FlowFactory that creates copies a from from one location to another. */
public class TestCacheFactory extends CascadeFactory
  {
  private String sourceString;
  private String sinkString;

  public TestCacheFactory( String name )
    {
    this( null, name );
    }

  public TestCacheFactory( Properties properties, String name )
    {
    super( properties, name );
    }

  public void setSource( String path )
    {
    sourceString = path;
    }

  public void setSink( String path )
    {
    sinkString = path;
    }

  @Override
  public Cascade create()
    {
    Collection<ConversionResource> resources = getResourcesWith( sourceString );

    for( ConversionResource resource : resources )
      {
      Collection<ProcessFactory> dependencies = getSourceDependenciesOn( resource );

      if( dependencies.isEmpty() || dependencies.size() < 2 ) // don't cache if more than one dep
        continue;

      ConversionResource cachedResource = new ConversionResource( sinkString, resource.getProtocol(), resource.getFormat(), resource.getMode() );

      TestCopyFactory cache = new TestCopyFactory( getProperties(), getName() + "-" + resource );

      cache.addSourceResource( resource );
      cache.addSinkResource( cachedResource );

      for( ProcessFactory dependency : dependencies )
        dependency.replaceSourceResource( resource, cachedResource );

      addProcessFactory( cache );
      }

    return super.create();
    }
  }
