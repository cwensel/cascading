/*
 * Copyright (c) 2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.splunk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Splunk helper utilities.
 */
public class SplunkUtil
  {
  private static final Logger LOG = LoggerFactory.getLogger( SplunkUtil.class );

  /**
   * Method loadSplunkRC will load a {@code .splunkrc} from either the location specified by
   * the env variable {@code SPLUNK_RC_HOME} or {@code HOME}.
   *
   * @return A Map of configuration key/value pairs.
   * @throws IOException if the file cannot be read.
   */
  public static Map<String, Object> loadSplunkRC() throws IOException
    {
    String home = System.getenv( "SPLUNK_RC_HOME" );

    if( home == null )
      home = System.getProperty( "user.home" );

    Path path = Paths.get( home + File.separator + ".splunkrc" );

    if( !Files.exists( path ) )
      return Collections.emptyMap();

    LOG.info( "reading .splunkrc from: {}", path );

    Map<String, Object> params = Files.readAllLines( path ).stream()
      .map( String::trim )
      .filter( line -> !line.isEmpty() )
      .filter( line -> !line.startsWith( "#" ) )
      .map( line -> line.split( "=" ) )
      .filter( pair -> pair.length == 2 )
      .filter( pair -> pair[ 1 ] != null )
      .collect( Collectors.toMap( pair -> pair[ 0 ], pair -> pair[ 1 ] ) );

    params.computeIfPresent( "port", ( k, v ) -> Integer.parseInt( v.toString() ) );

    return params;
    }
  }
