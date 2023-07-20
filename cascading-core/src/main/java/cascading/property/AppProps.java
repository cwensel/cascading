/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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
 * Class AppProps is a fluent helper for setting various application level properties that every
 * {@link cascading.flow.Flow} may or may not be required to have set. These properties are typically passed to a Flow
 * via a {@link cascading.flow.FlowConnector}.
 * <p>
 * New property settings that may be set in Cascading 2 are application name, version, and any tags.
 * <p>
 * See {@link #addTag(String)} for examples of using tags to help manage an application.
 * <p>
 * In prior releases, the FlowConnector was responsible for setting the "application jar" class or path. Those
 * methods have been deprecated and moved to AppProps.
 */
public class AppProps extends Props
  {
  private static final Logger LOG = LoggerFactory.getLogger( AppProps.class );

  public static final String APP_ID = "cascading.app.id";
  public static final String APP_NAME = "cascading.app.name";
  public static final String APP_VERSION = "cascading.app.version";
  public static final String APP_TAGS = "cascading.app.tags";
  public static final String APP_FRAMEWORKS = "cascading.app.frameworks";
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
  protected Set<String> frameworks = new TreeSet<String>();

  /**
   * Creates a new AppProps instance.
   *
   * @return AppProps instance
   */
  public static AppProps appProps()
    {
    return new AppProps();
    }

  /**
   * Method setApplicationJarClass is used to set the application jar file.
   * <p>
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
      PropertyUtil.setProperty( properties, APP_JAR_CLASS, type.getName() );
    }

  /**
   * Method getApplicationJarClass returns the Class set by the setApplicationJarClass method.
   *
   * @param properties of type Map
   * @return Class
   */
  public static Class<?> getApplicationJarClass( Map<Object, Object> properties )
    {
    // updated to retain compatibility with older versions
    Object type = PropertyUtil.getProperty( properties, DEP_APP_JAR_CLASS, null );

    if( type instanceof Class )
      {
      LOG.warn( "using deprecated property: {}, use instead: {}", DEP_APP_JAR_CLASS, APP_JAR_CLASS );
      return (Class<?>) type;
      }

    String className = (String) type;

    if( className != null )
      {
      LOG.warn( "using deprecated property: {}, use instead: {}", DEP_APP_JAR_CLASS, APP_JAR_CLASS );
      return Util.loadClassSafe( className );
      }

    type = PropertyUtil.getProperty( properties, APP_JAR_CLASS, null );

    if( type instanceof Class )
      return (Class<?>) type;

    className = (String) type;

    if( className == null )
      return null;

    return Util.loadClassSafe( className );
    }

  /**
   * Method setApplicationJarPath is used to set the application jar file.
   * <p>
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
   * @param properties of type Map
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
    properties.put( APP_ID, getAppID() );
    }

  public static String getApplicationID( Map<Object, Object> properties )
    {
    if( properties == null )
      return getAppID();

    return PropertyUtil.getProperty( properties, APP_ID, getAppID() );
    }

  public static String getApplicationID()
    {
    return getAppID();
    }

  private static String getAppID()
    {
    if( appID == null )
      {
      appID = Util.createUniqueID();
      LOG.info( "using app.id: {}", appID );
      }

    return appID;
    }

  /**
   * Sets the static appID value to null. For debugging purposes.
   */
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

    tag = tag.trim();

    if( Util.containsWhitespace( tag ) )
      LOG.warn( "tags should not contain whitespace characters: '{}'", tag );

    String tags = PropertyUtil.getProperty( properties, APP_TAGS, (String) null );

    if( tags != null )
      tags = join( ",", tag, tags );
    else
      tags = tag;

    properties.put( APP_TAGS, tags );
    }

  public static String getApplicationTags( Map<Object, Object> properties )
    {
    return PropertyUtil.getProperty( properties, APP_TAGS, (String) null );
    }

  /**
   * Adds a framework "name:version" string to the property set and to the System properties.
   * <p>
   * Properties may be null. Duplicates are removed.
   *
   * @param properties may be null, additionally adds to System properties
   * @param framework  "name:version" String
   */
  public static void addApplicationFramework( Map<Object, Object> properties, String framework )
    {
    if( framework == null )
      return;

    String frameworks = PropertyUtil.getProperty( properties, APP_FRAMEWORKS, System.getProperty( APP_FRAMEWORKS ) );

    if( frameworks != null )
      frameworks = join( ",", framework.trim(), frameworks );
    else
      frameworks = framework;

    frameworks = Util.unique( frameworks, "," );

    if( properties != null )
      properties.put( APP_FRAMEWORKS, frameworks );

    System.setProperty( APP_FRAMEWORKS, frameworks );
    }

  public static String getApplicationFrameworks( Map<Object, Object> properties )
    {
    return PropertyUtil.getProperty( properties, APP_FRAMEWORKS, System.getProperty( APP_FRAMEWORKS ) );
    }

  public AppProps()
    {
    }

  /**
   * Sets the name and version of this application.
   *
   * @param name    of type String
   * @param version of type String
   */
  public AppProps( String name, String version )
    {
    this.name = name;
    this.version = version;
    }

  /**
   * Method setName sets the application name.
   * <p>
   * By default the application name is derived from the jar name (values before the version in most Maven
   * compatible jars).
   *
   * @param name type String
   * @return this
   */
  public AppProps setName( String name )
    {
    this.name = name;

    return this;
    }

  /**
   * Method setVersion sets the application version.
   * <p>
   * By default the application version is derived from the jar name (values after the name in most Maven
   * compatible jars).
   *
   * @param version type String
   * @return this
   */
  public AppProps setVersion( String version )
    {
    this.version = version;

    return this;
    }

  public String getTags()
    {
    return join( tags, "," );
    }

  /**
   * Method addTag will associate a "tag" with this application. Applications can have an unlimited number of tags.
   * <p>
   * Tags allow applications to be searched and organized by management tools.
   * <p>
   * Tag values are opaque, but adopting a simple convention of 'category:value' allows for complex use cases.
   * <p>
   * Some recommendations for categories are:
   * <ul>
   * <li>cluster: - the cluster name the application is or should be run against. A name could be logical, like QA or PROD.</li>
   * <li>project: - the project name, possibly a JIRA project name this application is managed under.</li>
   * <li>org: - the group, team or organization that is responsible for the application.</li>
   * <li>support: - the email address of the user who should be notified of failures or issues.</li>
   * </ul>
   * <p>
   * Note that tags should not contain whitespace characters, even though this is not an error, a warning will be
   * issues.
   *
   * @param tag type String
   * @return this
   */
  public AppProps addTag( String tag )
    {
    if( !Util.isEmpty( tag ) )
      tags.add( tag );

    return this;
    }

  /**
   * Method addTags will associate the given "tags" with this application. Applications can have an unlimited number of tags.
   * <p>
   * Tags allow applications to be searched and organized by management tools.
   * <p>
   * Tag values are opaque, but adopting a simple convention of 'category:value' allows for complex use cases.
   * <p>
   * Some recommendations for categories are:
   * <ul>
   * <li>cluster: - the cluster name the application is or should be run against. A name could be logical, like QA or PROD.</li>
   * <li>project: - the project name, possibly a JIRA project name this application is managed under.</li>
   * <li>org: - the group, team or organization that is responsible for the application.</li>
   * <li>support: - the email address of the user who should be notified of failures or issues.</li>
   * </ul>
   * <p>
   * Note that tags should not contain whitespace characters, even though this is not an error, a warning will be
   * issues.
   *
   * @param tags type String
   * @return this
   */
  public AppProps addTags( String... tags )
    {
    for( String tag : tags )
      addTag( tag );

    return this;
    }

  /**
   * Method getFrameworks returns a list of frameworks used to build this App.
   *
   * @return Registered frameworks
   */
  public String getFrameworks()
    {
    return join( frameworks, "," );
    }

  /**
   * Method addFramework adds a new framework name to the list of frameworks used.
   * <p>
   * Higher level tools should register themselves, and preferably with their version,
   * for example {@code foo-flow-builder:1.2.3}.
   * <p>
   * See {@link #addFramework(String, String)}.
   *
   * @param framework A String
   * @return this AppProps instance
   */
  public AppProps addFramework( String framework )
    {
    if( !Util.isEmpty( framework ) )
      frameworks.add( framework );

    return this;
    }

  /**
   * Method addFramework adds a new framework name and its version to the list of frameworks used.
   * <p>
   * Higher level tools should register themselves, and preferably with their version,
   * for example {@code foo-flow-builder:1.2.3}.
   *
   * @param framework A String
   * @return this AppProps instance
   */
  public AppProps addFramework( String framework, String version )
    {
    if( !Util.isEmpty( framework ) && !Util.isEmpty( version ) )
      frameworks.add( framework + ":" + version );

    if( !Util.isEmpty( framework ) )
      frameworks.add( framework );

    return this;
    }

  /**
   * Method addFrameworks adds new framework names to the list of frameworks used.
   * <p>
   * Higher level tools should register themselves, and preferably with their version,
   * for example {@code foo-flow-builder:1.2.3}.
   *
   * @param frameworks Strings
   * @return this AppProps instance
   */
  public AppProps addFrameworks( String... frameworks )
    {
    for( String framework : frameworks )
      addFramework( framework );

    return this;
    }

  /**
   * Method setJarClass is used to set the application jar file.
   * <p>
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
   * <p>
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
    addApplicationFramework( properties, getFrameworks() );
    setApplicationJarClass( properties, jarClass );
    setApplicationJarPath( properties, jarPath );
    }
  }
