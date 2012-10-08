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

package cascading.property;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.join;

/**
 * Class AppPros is a fluent helper for setting various application level properties that every
 * {@link cascading.flow.Flow} may or may not be required to have set. These properties are typically passed to a Flow
 * via a {@link cascading.flow.FlowConnector}.
 * <p/>
 * In prior releases, the FlowConnector was responsible for setting the "application jar" class or path. Those
 * methods have been deprecated and moved to AppConfig.
 * <p/>
 * New property settings that may be set in Cascading 2 are application name, version, and any tags.
 */
public class AppProps extends Props
  {
  private static final Logger LOG = LoggerFactory.getLogger( AppProps.class );

  public static final String APP_ID = "cascading.app.id";
  public static final String APP_NAME = "cascading.app.name";
  public static final String APP_VERSION = "cascading.app.version";
  public static final String APP_TAGS = "cascading.app.tags";
  public static final String APP_JAR_CLASS = "cascading.app.appjar.class";
  public static final String APP_JAR_PATH = "cascading.app.appjar.path";

  static final String DEP_APP_JAR_CLASS = "cascading.flowconnector.appjar.class";
  static final String DEP_APP_JAR_PATH = "cascading.flowconnector.appjar.path";

  // need a global unique value here
  private static String appID;

  protected String name;
  protected String version;
  protected Set<String> tags = new TreeSet<String>();
  protected Class jarClass;
  protected String jarPath;

  public static AppProps appProps()
    {
    return new AppProps();
    }

  /**
   * Method setApplicationJarClass is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call setApplicationJarClass(java.util.Map, Class) or
   * {@link #setApplicationJarPath(java.util.Map, String)}, otherwise ClassNotFound exceptions are likely.
   *
   * @param properties of type Map
   * @param type       of type Class
   */
  public static void setApplicationJarClass( Map<Object, Object> properties, Class type )
    {
    if( type != null )
      properties.put( APP_JAR_CLASS, type );
    }

  /**
   * Method getApplicationJarClass returns the Class set by the setApplicationJarClass method.
   *
   * @param properties of type Map<Object, Object>
   * @return Class
   */
  public static Class getApplicationJarClass( Map<Object, Object> properties )
    {
    Class property = PropertyUtil.getProperty( properties, DEP_APP_JAR_CLASS, (Class) null );

    if( property != null )
      {
      LOG.warn( "using deprecated property: {}, use instead: {}", DEP_APP_JAR_CLASS, APP_JAR_CLASS );
      return property;
      }

    return PropertyUtil.getProperty( properties, APP_JAR_CLASS, (Class) null );
    }

  /**
   * Method setApplicationJarPath is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call {@link #setApplicationJarClass(java.util.Map, Class)} or
   * setApplicationJarPath(java.util.Map, String), otherwise ClassNotFound exceptions are likely.
   *
   * @param properties of type Map
   * @param path       of type String
   */
  public static void setApplicationJarPath( Map<Object, Object> properties, String path )
    {
    if( path != null )
      properties.put( APP_JAR_PATH, path );
    }

  /**
   * Method getApplicationJarPath return the path set by the setApplicationJarPath method.
   *
   * @param properties of type Map<Object, Object>
   * @return String
   */
  public static String getApplicationJarPath( Map<Object, Object> properties )
    {
    String property = PropertyUtil.getProperty( properties, DEP_APP_JAR_PATH, (String) null );

    if( property != null )
      {
      LOG.warn( "using deprecated property: {}, use instead: {}", DEP_APP_JAR_PATH, APP_JAR_PATH );
      return property;
      }

    return PropertyUtil.getProperty( properties, APP_JAR_PATH, (String) null );
    }

  public static void setApplicationID( Map<Object, Object> properties )
    {
    properties.put( APP_ID, getAppID( properties ) );
    }

  public static String getApplicationID( Map<Object, Object> properties )
    {
    if( properties == null )
      return getAppID( null );

    return PropertyUtil.getProperty( properties, APP_ID, getAppID( properties ) );
    }

  private static String getAppID( Map<Object, Object> properties )
    {
    if( appID == null )
      {
      String appName = properties == null ? "appnameseed" : getApplicationName( properties );
      appID = Util.createUniqueID();
      LOG.info( "using app.id: {}", appID );
      }

    return appID;
    }

  /** Sets the static appID value to null. For debugging purposes. */
  public static void resetAppID()
    {
    appID = null;
    }

  public static void setApplicationName( Map<Object, Object> properties, String name )
    {
    if( name != null )
      properties.put( APP_NAME, name );
    }

  public static String getApplicationName( Map<Object, Object> properties )
    {
    return PropertyUtil.getProperty( properties, APP_NAME, (String) null );
    }

  public static void setApplicationVersion( Map<Object, Object> properties, String version )
    {
    if( version != null )
      properties.put( APP_VERSION, version );
    }

  public static String getApplicationVersion( Map<Object, Object> properties )
    {
    return PropertyUtil.getProperty( properties, APP_VERSION, (String) null );
    }

  public static void addApplicationTag( Map<Object, Object> properties, String tag )
    {
    if( tag == null )
      return;

    String tags = PropertyUtil.getProperty( properties, APP_TAGS, (String) null );

    if( tags != null )
      tags = join( ",", tag.trim(), tags );
    else
      tags = tag;

    properties.put( APP_TAGS, tags );
    }

  public static String getApplicationTags( Map<Object, Object> properties )
    {
    return PropertyUtil.getProperty( properties, APP_TAGS, (String) null );
    }

  public AppProps()
    {
    }

  public AppProps( String name, String version )
    {
    this.name = name;
    this.version = version;
    }

  public AppProps setName( String name )
    {
    this.name = name;

    return this;
    }

  public AppProps setVersion( String version )
    {
    this.version = version;

    return this;
    }

  public String getTags()
    {
    return join( tags, "," );
    }

  public AppProps addTag( String tag )
    {
    if( !Util.isEmpty( tag ) )
      tags.add( tag );

    return this;
    }

  public AppProps addTags( String... tags )
    {
    for( String tag : tags )
      addTag( tag );

    return this;
    }

  /**
   * Method setJarClass is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call setApplicationJarClass(java.util.Map, Class) or
   * {@link #setApplicationJarPath(java.util.Map, String)}, otherwise ClassNotFound exceptions are likely.
   *
   * @param jarClass of type Class
   */
  public AppProps setJarClass( Class jarClass )
    {
    this.jarClass = jarClass;

    return this;
    }

  /**
   * Method setJarPath is used to set the application jar file.
   * </p>
   * All cluster executed Cascading applications
   * need to call {@link #setJarClass(Class)} or
   * setJarPath(java.util.Map, String), otherwise ClassNotFound exceptions are likely.
   *
   * @param jarPath of type String
   */
  public AppProps setJarPath( String jarPath )
    {
    this.jarPath = jarPath;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setApplicationID( properties );
    setApplicationName( properties, name );
    setApplicationVersion( properties, version );
    addApplicationTag( properties, getTags() );
    setApplicationJarClass( properties, jarClass );
    setApplicationJarPath( properties, jarPath );
    }
  }
