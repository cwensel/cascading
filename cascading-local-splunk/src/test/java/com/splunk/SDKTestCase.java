/*
 * Copyright 2011 Splunk, Inc.
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

/*
 * This code was copied from https://github.com/splunk/splunk-sdk-java
 *
 *  Since no test artifacts were published.
 */

package com.splunk;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * Base test case for SDK test suite.
 * <p>
 * TestCase does the following actions in the various test contexts:
 *
 * @@Before: - Read ~/.splunkrc to get host, port, user, and password to connect with.
 * - Create a Service object connected to the Splunk instance.
 * @@After: - Logout the Service object.
 */
public abstract class SDKTestCase
  {
  protected static final boolean WORKAROUND_KNOWN_BUGS = true;
  private static final boolean VERBOSE_PORT_SCAN = false;

  protected static Service service;
  protected List<String> installedApps;

  protected Command command;

  public static String streamToString( InputStream is )
    {
    Reader r = null;
    try
      {
      r = new InputStreamReader( is, "UTF-8" );
      }
    catch( UnsupportedEncodingException e )
      {
      throw new RuntimeException( "Your JVM does not support UTF-8!" );
      }
    StringBuffer sb = new StringBuffer();
    int ch;
    try
      {
      while( ( ch = r.read() ) != -1 )
        {
        sb.append( (char) ch );
        }
      }
    catch( IOException ioex )
      {
      throw new RuntimeException( ioex.getMessage() );
      }
    return sb.toString();
    }

  public void connect()
    {
    if( service != null )
      {
      service.login( service.username, service.password );
      }
    else
      {
      service = Service.connect( command.opts );
      }
    }

  public static Integer getJavaVersion()
    {
    String ver = System.getProperty( "java.version" );
    return Integer.parseInt( ver.substring( 2, 3 ) );
    }

  @Before
  public void setUp() throws Exception
    {
    // If using Charles Proxy for debugging, uncomment these lines.
    //System.setProperty("https.proxyHost", "127.0.0.1");
    //System.setProperty("https.proxyPort", "8888");

    // TLSv1_2 required for splunk 7
    HttpService.setSslSecurityProtocol( SSLSecurityProtocol.TLSv1_2 );

    if( service == null )
      command = Command.splunk();

    connect();
    if( restartRequired() )
      {
      System.out.println(
        "WARNING: Splunk was already in a state requiring a " +
          "restart prior to running this test. Trying to recover..." );
      splunkRestart();
      System.out.println( "Restart complete." );
      }

    installedApps = new ArrayList<String>();
    }

  @After
  public void tearDown() throws Exception
    {
    if( restartRequired() )
      {
      Assert.fail( "Test left Splunk in a state that required restart." );
      }

    // Cannot delete applications in Splunk 4.2.
    if( service.versionIsAtLeast( "4.3" ) )
      {
      // We delete the apps we installed for capabilities after checking
      // for restarts that might be required, since deleting an app
      // triggers a restart (but one that we can safely ignore).
      for( String applicationName : installedApps )
        {
        try
          {
          service.getApplications().remove( applicationName );
          }
        catch( HttpException e )
          {
          // WORKAROUND (SPL-75224): Under Splunk 6.0 on Windows, deleting apps sometimes fails with
          // the message "Operation completed successfully." The app is actually deleted, but it will
          // cause tests to fail.
          if( service.versionCompare( "6.0.0" ) == 0 && e.getStatus() != 500 )
            {
            throw e;
            }
          }
        clearRestartMessage();
        }
      }
    }

  // === Temporary Names ===

  protected static String createTimestamp()
    {
    DateFormat dateFormat = new SimpleDateFormat( "yyyy/MM/dd-HH:mm:ss" );
    return dateFormat.format( new Date() );
    }

  protected static String createTemporaryName()
    {
    return "delete-me-" + UUID.randomUUID().toString();
    }

  public static InputStream openResource( String path )
    {
    if( path.startsWith( "splunk_search:" ) )
      {
      path = path.substring( "splunk_search:".length() );

      String[] pathComponents = path.split( "/" );
      String searchType = pathComponents[ 0 ];
      String outputMode = pathComponents[ 1 ];
      String search = pathComponents[ 2 ];

      Args resultsArgs = new Args( "output_mode", outputMode );
      if( searchType.equals( "blocking" ) )
        {
        Job job = service.getJobs().create(
          search,
          new Args( "exec_mode", "blocking" ) );
        return job.getResults( resultsArgs );
        }
      else if( searchType.equals( "oneshot" ) )
        {
        return service.oneshotSearch( search, resultsArgs );
        }
      else
        {
        throw new IllegalArgumentException(
          "Unrecognized search type: " + searchType );
        }
      }

    InputStream input = ResourceRoot.class.getResourceAsStream( path );
    Assert.assertNotNull( "Could not open " + path, input );
    return input;
    }

  // === Asserts ===

  public interface EventuallyTrueBehavior extends Supplier<Boolean>
    {
    default int tries()
      {
      return 60;
      }

    default int pauseTime()
      {
      return 1000;
      }

    default String message()
      {
      return "Test timed out before true.";
      }
    }

  public static boolean assertEventuallyTrue( EventuallyTrueBehavior behavior )
    {
    int remainingTries = behavior.tries();
    while( remainingTries > 0 )
      {
      boolean succeeded = behavior.get();
      if( succeeded )
        {
        return true;
        }
      else
        {
        remainingTries -= 1;
        try
          {
          Thread.sleep( behavior.pauseTime() );
          }
        catch( InterruptedException e ){}
        }
      }
    Assert.fail( behavior.message() );
    return false;
    }

  // === Test Data Installation ===

  public boolean hasTestData()
    {
    String collectionName = "sdk-app-collection";
    return service.getApplications().containsKey( "sdk-app-collection" );
    }

  public void installApplicationFromTestData( String applicationName )
    {
    String collectionName = "sdk-app-collection";
    if( !service.getApplications().containsKey( "sdk-app-collection" ) )
      {
      throw new RuntimeException( "no apps instaled" );
      }

    String splunkHome = service.getSettings().getSplunkHome();

    // This is the filename separator sequence for splunkd, not
    // the Splunk SDK. Therefore we can't just use File.separator here.
    String separator;
    if( splunkHome.contains( "/" ) && splunkHome.contains( "\\" ) )
      {
      // Windows - allows mixed paths
      separator = "\\";
      }
    else if( splunkHome.contains( "/" ) )
      {
      // Unix or Mac OS X
      separator = "/";
      }
    else if( splunkHome.contains( "\\" ) )
      {
      // Windows
      separator = "\\";
      }
    else
      {
      throw new RuntimeException(
        "Couldn't determine what the path separator was " +
          "for splunkd." );
      }

    String[] pathComponents = {splunkHome, "etc", "apps",
                               collectionName, "build", applicationName + ".tar"};
    String appPath = Util.join( separator, pathComponents );

    Args args = new Args();
    args.put( "name", appPath );
    args.put( "update", "1" );
    service.post( "apps/appinstall", args );

    installedApps.add( applicationName );
    }

  // === Restarts ===

  public void clearRestartMessage()
    {
    final MessageCollection messages = service.getMessages();
    if( messages.containsKey( "restart_required" ) )
      {
      messages.remove( "restart_required" );
      assertEventuallyTrue( () ->
      {
      messages.refresh();
      return !messages.containsKey( "restart_required" );
      } );
      }
    }

  public boolean restartRequired()
    {
    for( Message message : service.getMessages().values() )
      {
      if( message.containsKey( "restart_required" ) )
        {
        return true;
        }
      }
    return false;
    }

  private void markAsRestartRequired()
    {
    service.getMessages().create(
      "restart_required",
      "Unit test called markAsRestartRequired()." );
    }

  public void splunkRestart()
    {
    if( !restartRequired() )
      {
      Assert.fail( "Asked to restart Splunk when no restart was required." );
      }
    uncheckedSplunkRestart();
    }

  public void uncheckedSplunkRestart()
    {
    ResponseMessage response = service.restart();
    if( response.getStatus() != 200 )
      {
      Assert.fail( "Restart command failed: " + response.getContent() );
      }

    // (This status will be cleared when Splunk actually restarts.)
    markAsRestartRequired();

    // Wait for splunkd to come back up fresh.
    // (And update 'service'.)
    assertEventuallyTrue( () ->
    {
    try
      {
      connect();
      return !restartRequired();
      }
    catch( Exception e )
      {
      return false;
      }
    } );
    }

  // === Misc ===

  protected static void sleep( int milliseconds )
    {
    try
      {
      Thread.sleep( milliseconds );
      }
    catch( InterruptedException e ){}
    }

  protected int findNextUnusedPort( int startingPort )
    {
    int port = startingPort;
    while( isPortInUse( port ) )
      {
      port++;
      }
    return port;
    }

  public boolean isPortInUse( int port )
    {
    try
      {
      Socket pingSocket = new Socket();
      // On Windows, the firewall doesn't respond at all if you connect to an unbound port, so we need to
      // take lack of a connection as an empty port. Timeout is 1000ms.
      try
        {
        pingSocket.connect( new InetSocketAddress( service.getHost(), port ), 1000 );
        }
      catch( SocketTimeoutException ste )
        {
        return false;
        }
      pingSocket.close();
      if( VERBOSE_PORT_SCAN )
        {
        System.out.println( "IN-USE(" + port + ")" );
        }
      return true;
      }
    catch( IOException e )
      {
      if( VERBOSE_PORT_SCAN )
        {
        System.out.println( "OPEN(" + port + "): " + e.getMessage() );
        }
      return false;
      }
    }

  protected static boolean contains( String[] array, String value )
    {
    return Arrays.asList( array ).contains( value );
    }

  protected static String locateSystemLog()
    {
    String filename = null;
    String osName = service.getInfo().getOsName();
    if( osName.equals( "Windows" ) )
      filename = "C:\\Windows\\WindowsUpdate.log";
    else if( osName.equals( "Linux" ) )
      {
      filename = "/etc/fstab";
      }
    else if( osName.equals( "Darwin" ) )
      {
      filename = "/var/log/system.log";
      }
    else
      {
      throw new RuntimeException( "OS " + osName + " not recognized" );
      }

    Assert.assertNotNull( filename );
    return filename;
    }

  protected static String joinServerPath( String[] components )
    {
    String separator;
    String osName = service.getInfo().getOsName();
    if( osName.equals( "Windows" ) )
      separator = "\\";
    else if( osName.equals( "Linux" ) )
      separator = "/";
    else if( osName.equals( "Darwin" ) )
      {
      separator = "/";
      }
    else
      {
      throw new RuntimeException( "OS " + osName + " not recognized" );
      }

    return Util.join( separator, components );
    }

  protected boolean firstLineIsXmlDtd( InputStream stream )
    {
    InputStreamReader reader;
    try
      {
      reader = new InputStreamReader( stream, "UTF-8" );
      }
    catch( UnsupportedEncodingException e )
      {
      throw new Error( e );
      }
    BufferedReader lineReader = new BufferedReader( reader );
    try
      {
      return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>".equals(
        lineReader.readLine()
      );
      }
    catch( IOException e )
      {
      Assert.fail( e.toString() );
      return false;
      }
    }

  // Added

  protected int getResultCountOfIndex( Service service, String indexName )
    {
    InputStream results = service.oneshotSearch( "search index=" + indexName );

    BufferedInputStream stream = new BufferedInputStream( results );

    try
      {
      stream.mark( 2048 );

      ResultsReaderXml resultsReader = new ResultsReaderXml( stream );

      int numEvents = 0;

      while( resultsReader.getNextEvent() != null )
        numEvents++;

      return numEvents;
      }
    catch( Exception e )
      {
      try
        {
        stream.reset();

        BufferedReader r = new BufferedReader(
          new InputStreamReader( stream, StandardCharsets.UTF_8 ) );

        String line;

        while( ( line = r.readLine() ) != null )
          {
          System.out.println( "line = " + line );
          }
        }
      catch( IOException exception )
        {
        System.out.println( "exception = " + exception );
        }

      throw new RuntimeException( e );
      }
    }

  protected int getTotalEventCount( Index index )
    {
    index.refresh();
    return index.getTotalEventCount();
    }
  }
