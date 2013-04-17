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

package cascading.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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

    InputStream stream = Version.class.getClassLoader().getResourceAsStream( "cascading/version.properties" );

    if( stream == null )
      return properties;

    properties.load( stream );

    stream = Version.class.getClassLoader().getResourceAsStream( "cascading/build.number.properties" );

    if( stream != null )
      properties.load( stream );

    return properties;
    }
  }
