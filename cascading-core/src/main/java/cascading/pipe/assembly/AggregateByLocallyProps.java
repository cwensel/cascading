/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.pipe.assembly;

import java.util.Properties;

import cascading.property.Props;
import cascading.util.cache.BaseCacheFactory;
import cascading.util.cache.LRUHashMapCacheFactory;

/**
 * Class AggregateByLocallyProps is a fluent helper for setting various properties related to the cache used in {@link AggregateByLocally}.
 */
public class AggregateByLocallyProps extends Props
  {
  /** property to control the cache factory used in AggregateBy. */
  public static String AGGREGATE_LOCALLY_BY_CACHE_FACTORY = "cascading.aggregateby.locally.cachefactory.classname";

  /** property to control the capacity of the cache to use. */
  public static final String AGGREGATE_LOCALLY_BY_CAPACITY = "cascading.aggregateby.locally.cache.capacity";

  /** default capacity of caches used in AggregateBy. */
  public static int AGGREGATE_LOCALLY_BY_DEFAULT_CAPACITY = BaseCacheFactory.DEFAULT_CAPACITY;

  /** default factory class for creating caches. */
  public static final Class<? extends BaseCacheFactory> DEFAULT_CACHE_FACTORY_CLASS = LRUHashMapCacheFactory.class;

  private Properties properties;

  public static AggregateByLocallyProps aggregateByLocallyProps()
    {
    return new AggregateByLocallyProps();
    }

  public AggregateByLocallyProps()
    {
    this.properties = new Properties();
    }

  /**
   * Sets the CacheFactory class to use.
   *
   * @param cacheFactory The cache factory class to use.
   */
  public AggregateByLocallyProps setCacheFactoryClass( Class<? extends BaseCacheFactory> cacheFactory )
    {
    return setCacheFactoryClassName( cacheFactory.getName() );
    }

  /**
   * Sets the name of the CacheFactory class to use.
   *
   * @param cacheFactoryClassName The full name of the cache factory class to use.
   */
  public AggregateByLocallyProps setCacheFactoryClassName( String cacheFactoryClassName )
    {
    properties.setProperty( AGGREGATE_LOCALLY_BY_CACHE_FACTORY, cacheFactoryClassName );
    return this;
    }

  /**
   * Sets the capacity of the cache.
   *
   * @param capacity The capacity of the cache.
   */
  public AggregateByLocallyProps setCapacity( int capacity )
    {
    properties.setProperty( AGGREGATE_LOCALLY_BY_CAPACITY, String.valueOf( capacity ) );
    return this;
    }

  /**
   * Returns the capacity.
   *
   * @return The capacity.
   */
  public int getCapacity()
    {
    String capacityValue = properties.getProperty( AGGREGATE_LOCALLY_BY_CAPACITY );

    if( capacityValue == null )
      return BaseCacheFactory.DEFAULT_CAPACITY;

    return Integer.valueOf( capacityValue );
    }

  /**
   * Returns the name of the cache factory.
   *
   * @return The cache class name.
   */
  public String getCacheFactoryClassName()
    {
    String className = properties.getProperty( AGGREGATE_LOCALLY_BY_CACHE_FACTORY );

    if( className == null )
      return DEFAULT_CACHE_FACTORY_CLASS.getName();

    return className;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    properties.putAll( this.properties );
    }
  }
