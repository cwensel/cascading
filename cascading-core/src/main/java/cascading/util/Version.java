/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Version
  {
  private static final Logger LOG = LoggerFactory.getLogger( Version.class );

  private static boolean printedVersion = false;

  public static final String CASCADING_RELEASE_MAJOR = "cascading.release.major";
  public static final String CASCADING_RELEASE_MINOR = "cascading.release.minor";
  public static final String CASCADING_BUILD_NUMBER = "cascading.build.number";
  public static final String CASCADING = "Cascading";

  public static Properties versionProperties;

  private static synchronized Properties getVersionProperties()
    {
    try
      {
      if( versionProperties == null )
        {
        versionProperties = loadVersionProperties();

        if( versionProperties.isEmpty() )
          LOG.warn( "unable to load version information" );
        }
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load version information", exception );
      versionProperties = new Properties();
      }

    return versionProperties;
    }

  public static synchronized void printBanner()
    {
    // only print once
    if( printedVersion )
      return;

    printedVersion = true;

    String version = getVersionString();

    if( version != null )
      LOG.info( version );
    }

  public static String getVersionString()
    {
    if( getVersionProperties().isEmpty() )
      return null;

    String releaseVersion;

    if( getReleaseBuild() == null || getReleaseBuild().isEmpty() )
      releaseVersion = String.format( "Concurrent, Inc - %s %s", CASCADING, getReleaseFull() );
    else
      releaseVersion = String.format( "Concurrent, Inc - %s %s-%s", CASCADING, getReleaseFull(), getReleaseBuild() );

    return releaseVersion;
    }

  public static String getRelease()
    {
    if( getVersionProperties().isEmpty() )
      return null;

    if( getReleaseBuild() == null || getReleaseBuild().isEmpty() )
      return String.format( "%s", getReleaseFull() );
    else
      return String.format( "%s-%s", getReleaseFull(), getReleaseBuild() );
    }

  public static String getReleaseFull()
    {
    String releaseFull;

    if( getReleaseMinor() == null || getReleaseMinor().isEmpty() )
      releaseFull = getReleaseMajor();
    else
      releaseFull = String.format( "%s.%s", getReleaseMajor(), getReleaseMinor() );

    return releaseFull;
    }

  public static boolean hasMajorMinorVersionInfo()
    {
    return !Util.isEmpty( getReleaseMinor() ) && !Util.isEmpty( getReleaseMajor() );
    }

  public static boolean hasAllVersionInfo()
    {
    return !Util.isEmpty( getReleaseBuild() ) && hasMajorMinorVersionInfo();
    }

  public static String getReleaseBuild()
    {
    return getVersionProperties().getProperty( CASCADING_BUILD_NUMBER );
    }

  public static String getReleaseMinor()
    {
    return getVersionProperties().getProperty( CASCADING_RELEASE_MINOR );
    }

  public static String getReleaseMajor()
    {
    return getVersionProperties().getProperty( CASCADING_RELEASE_MAJOR );
    }

  public static Properties loadVersionProperties() throws IOException
    {
    Properties properties = new Properties();

    List<URL> resources = Collections.list( Version.class.getClassLoader().getResources( "cascading/version.properties" ) );

    if( resources.isEmpty() )
      return properties;

    warnOnDuplicate( resources );

    InputStream stream = resources.get( 0 ).openStream();

    if( stream == null )
      return properties;

    try
      {
      properties.load( stream );
      }
    finally
      {
      stream.close();
      }

    stream = Version.class.getClassLoader().getResourceAsStream( "cascading/build.number.properties" );

    if( stream != null )
      {
      try
        {
        properties.load( stream );
        }
      finally
        {
        stream.close();
        }
      }

    return properties;
    }

  /**
   * A shaded jar will have multiple version.properties, e.g.
   * <pre>
   * file:/mnt/var/lib/hadoop/tmp/hadoop-unjar7817209360894770970/cascading/version.properties
   * jar:file:/mnt/single-load/./load-hadoop2-tez-20150729.jar!/cascading/version.properties
   * </pre>
   * <p/>
   * only warn if there are duplicates within a protocol, not across since we should only be seeing file: and jar:
   */
  private static void warnOnDuplicate( List<URL> resources )
    {
    if( resources.size() == 1 )
      return;

    SetMultiMap<String, String> map = new SetMultiMap<>();

    for( URL resource : resources )
      map.put( resource.getProtocol(), resource.toString() );

    for( String key : map.getKeys() )
      {
      Set<String> values = map.getValues( key );

      if( values.size() > 1 )
        LOG.warn( "found multiple 'cascading/version.properties' files on the CLASSPATH. Please check your dependencies: {}, using first returned", Util.join( values, "," ) );
      }
    }
  }
