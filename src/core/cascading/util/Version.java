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
      String hadoopVersion = versionProperties.getProperty( "cascading.hadoop.compatible.version" );
      String releaseFull = null;

      if( releaseMinor == null )
        releaseFull = releaseMajor;
      else
        releaseFull = String.format( "%s.%s", releaseMajor, releaseMinor );

      String message = null;

      if( releaseBuild == null )
        message = String.format( "Concurrent, Inc - Cascading %s [%s]", releaseFull, hadoopVersion );
      else
        message = String.format( "Concurrent, Inc - Cascading %s%s [%s]", releaseFull, releaseBuild, hadoopVersion );

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
