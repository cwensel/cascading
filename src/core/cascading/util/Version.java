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
    if( versionProperties != null )
      return;

    try
      {
      versionProperties = loadVersionProperties();

      if( versionProperties.isEmpty() )
        {
        LOG.warn( "unable to load version information" );
        return;
        }

      String releaseMajor = versionProperties.getProperty( "cascading.release.major" );
      String releaseMinor = versionProperties.getProperty( "cascading.release.minor", null );
      String releaseBuild = versionProperties.getProperty( "build.number", null );
      String platformVersion = versionProperties.getProperty( "cascading.platform.compatible.version" );
      String releaseFull = null;

      if( releaseMinor == null )
        releaseFull = releaseMajor;
      else
        releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );

      String message = null;

      if( releaseBuild == null )
        message = String.format( "Concurrent, Inc - Cascading %s [%s]", releaseFull, platformVersion );
      else
        message = String.format( "Concurrent, Inc - Cascading %s-%s [%s]", releaseFull, releaseBuild, platformVersion );

      LOG.info( message );
      }
    catch( IOException exception )
      {
      LOG.warn( "unable to load version information", exception );
      }
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
