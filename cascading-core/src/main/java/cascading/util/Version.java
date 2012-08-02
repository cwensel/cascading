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

  /** Field versionProperties */
  public static Properties versionProperties;

  public static synchronized void printBanner()
    {
    // only print once
    if( versionProperties != null )
      return;

    String version = getVersionString();

    if( version != null )
      LOG.info( version );
    }

  public static String getVersionString()
    {
    try
      {
      if( versionProperties == null )
        versionProperties = loadVersionProperties();
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load version information", exception );
      return null;
      }

    if( versionProperties.isEmpty() )
      {
      LOG.warn( "unable to load version information" );
      return null;
      }

    String releaseMajor = versionProperties.getProperty( "cascading.release.major" );
    String releaseMinor = versionProperties.getProperty( "cascading.release.minor" );
    String releaseBuild = versionProperties.getProperty( "cascading.build.number" );

    String releaseFull = null;

    if( releaseMinor == null || releaseMinor.isEmpty() )
      releaseFull = releaseMajor;
    else
      releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );

    String releaseVersion = null;

    if( releaseBuild == null || releaseBuild.isEmpty() )
      releaseVersion = String.format( "Concurrent, Inc - Cascading %s", releaseFull );
    else
      releaseVersion = String.format( "Concurrent, Inc - Cascading %s-%s", releaseFull, releaseBuild );

    return releaseVersion;
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
