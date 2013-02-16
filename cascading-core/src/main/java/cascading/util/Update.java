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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import cascading.flow.planner.PlatformInfo;
import cascading.property.AppProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class Update extends TimerTask
  {
  private static final Logger LOG = LoggerFactory.getLogger( Update.class );
  private static final String UPDATE_PROPERTIES = "latest.properties";

  public static final String UPDATE_CHECK_SKIP = "cascading.update.skip";
  public static final String UPDATE_URL = "cascading.update.url";

  private static final Set<PlatformInfo> platformInfoSet = Collections.synchronizedSet( new TreeSet<PlatformInfo>() );
  private static Timer timer;

  public static synchronized void checkForUpdate( PlatformInfo platformInfo )
    {
    if( Boolean.getBoolean( UPDATE_CHECK_SKIP ) )
      return;

    if( platformInfo != null )
      platformInfoSet.add( platformInfo );

    if( timer != null )
      return;

    timer = new Timer( "UpdateRequestTimer", true );
    timer.scheduleAtFixedRate( new Update(), 1000 * 30, 24 * 60 * 60 * 1000L );
    }

  @Override
  public void run()
    {
    checkForUpdate();
    }

  public boolean checkForUpdate()
    {
    if( !Version.hasMajorMinorVersionInfo() )
      return true;

    boolean isCurrentWip = Version.getReleaseFull() != null && Version.getReleaseFull().contains( "wip" );
    boolean isCurrentDev = isCurrentWip ? Version.getReleaseFull().contains( "wip-dev" ) : Version.getReleaseBuild() == null;

    URL updateCheckUrl = getUpdateCheckUrl();

    if( updateCheckUrl == null )
      return false;

    // do this before fetching latest.properties
    if( isCurrentDev )
      {
      LOG.debug( "current release is dev build, update url: {}", updateCheckUrl.toString() );
      return true;
      }

    Properties latestProperties = getUpdateProperties( updateCheckUrl );

    if( latestProperties.isEmpty() )
      return false;

    String latestMajor = latestProperties.getProperty( Version.CASCADING_RELEASE_MAJOR );
    String latestMinor = latestProperties.getProperty( Version.CASCADING_RELEASE_MINOR );

    boolean isSameMajorRelease = equals( Version.getReleaseMajor(), latestMajor );
    boolean isSameMinorRelease = equals( Version.getReleaseMinor(), latestMinor );

    if( isSameMajorRelease && isSameMinorRelease )
      {
      LOG.debug( "no updates available" );
      return true;
      }

    String version = latestProperties.getProperty( "cascading.release.version" );

    if( version == null )
      LOG.debug( "release version info not found" );
    else
      LOG.info( "newer Cascading release available: {}", version );

    return true;
    }

  private static Properties getUpdateProperties( URL updateUrl )
    {
    try
      {
      URLConnection connection = updateUrl.openConnection();
      connection.setConnectTimeout( 3 * 1000 );

      InputStream in = connection.getInputStream();

      try
        {
        Properties props = new Properties();

        props.load( connection.getInputStream() );

        return props;
        }
      finally
        {
        if( in != null )
          close( in );
        }
      }
    catch( IOException exception )
      {
      LOG.debug( "unable to fetch latest properties", exception );
      return new Properties();
      }
    }

  private static URL getUpdateCheckUrl()
    {
    String url = buildURL();

    String connector = url.indexOf( '?' ) > 0 ? "&" : "?";

    String spec = url + connector + buildParamsString();

    try
      {
      return new URL( spec );
      }
    catch( MalformedURLException exception )
      {
      LOG.debug( "malformed url: {}", spec, exception );
      return null;
      }
    }

  private static String buildURL()
    {
    String baseURL = System.getProperty( UPDATE_URL, "" );

    if( baseURL.isEmpty() )
      {
      String releaseBuild = Version.getReleaseBuild();

      if( releaseBuild == null || releaseBuild.contains( "wip" ) )
        baseURL = "http://files.concurrentinc.com/cascading/";
      else
        baseURL = "http://files.cascading.org/cascading/";
      }

    if( !baseURL.endsWith( "/" ) )
      baseURL += "/";

    baseURL = String.format( "%s%s/%s", baseURL, Version.getReleaseMajor(), UPDATE_PROPERTIES );

    return baseURL;
    }

  private static String buildParamsString()
    {
    StringBuilder sb = new StringBuilder();

    sb.append( "id=" );
    sb.append( getClientId() );
    sb.append( "&instance=" );
    sb.append( urlEncode( AppProps.getApplicationID( null ) ) );
    sb.append( "&os-name=" );
    sb.append( urlEncode( getProperty( "os.name" ) ) );
    sb.append( "&jvm-name=" );
    sb.append( urlEncode( getProperty( "java.vm.name" ) ) );
    sb.append( "&jvm-version=" );
    sb.append( urlEncode( getProperty( "java.version" ) ) );
    sb.append( "&os-arch=" );
    sb.append( urlEncode( getProperty( "os.arch" ) ) );
    sb.append( "&product=" );
    sb.append( urlEncode( Version.CASCADING ) );
    sb.append( "&version=" );
    sb.append( urlEncode( Version.getReleaseFull() ) );
    sb.append( "&version-build=" );
    sb.append( urlEncode( Version.getReleaseBuild() ) );
    sb.append( "&frameworks=" );
    sb.append( urlEncode( getProperty( AppProps.APP_FRAMEWORKS ) ) );

    synchronized( platformInfoSet )
      {
      for( PlatformInfo platformInfo : platformInfoSet )
        {
        sb.append( "&platform-name=" );
        sb.append( urlEncode( platformInfo.name ) );
        sb.append( "&platform-version=" );
        sb.append( urlEncode( platformInfo.version ) );
        sb.append( "&platform-vendor=" );
        sb.append( urlEncode( platformInfo.vendor ) );
        }

      platformInfoSet.clear();
      }

    return sb.toString();
    }

  private static boolean equals( String lhs, String rhs )
    {
    return lhs != null && lhs.equals( rhs );
    }

  private static int getClientId()
    {
    try
      {
      return Math.abs( InetAddress.getLocalHost().hashCode() );
      }
    catch( Throwable t )
      {
      return 0;
      }
    }

  private static String urlEncode( String param )
    {
    if( param == null )
      return "";

    try
      {
      return URLEncoder.encode( param, "UTF-8" );
      }
    catch( UnsupportedEncodingException exception )
      {
      LOG.debug( "unable to encode param: {}", param, exception );

      return null;
      }
    }

  private static String getProperty( String prop )
    {
    return System.getProperty( prop, "" );
    }

  private static void close( InputStream in )
    {
    try
      {
      in.close();
      }
    catch( IOException exception )
      {
      // do nothing
      }
    }
  }
