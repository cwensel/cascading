/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.management.annotation;

import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URISanitizer is an implementation of the Sanitizer interface to sanitize URIs of different kinds
 * (file, HTTP, HDFS, JDBC etc.) Depending on the visibility, the Sanitizer will return different values:
 * <p>
 * For hierarchical URIs (jdbc://...):
 * <ul>
 * <li>PUBLIC: Only return the path of the URI</li>
 * <li>PROTECTED: Same as PUBLIC + query parameters</li>
 * <li>PRIVATE: Same as PROTECTED + URI scheme and authority (host/port)</li>
 * </ul>
 * <p>
 * For opaque URIs (mailto:someone@email.com):
 * <ul>
 * <li>PUBLIC: Only return the scheme of the URI, 'mailto:' etc</li>
 * <li>PROTECTED: Same as PUBLIC</li>
 * <li>PRIVATE: The whole URI</li>
 * </ul>
 * <p>
 * Parameters containing sensitive information like user-names, passwords, API-keys etc. can be filtered out by setting
 * the {@link cascading.management.annotation.URISanitizer#PARAMETER_FILTER_PROPERTY} System property to a comma separated
 * list of names that should never show up in the {@link cascading.management.DocumentService}. Some systems may use
 * non-standard URIs, which cannot be parsed by {@link java.net.URI}.
 * <p>
 * If the sanitizer encounters one of those URIs it
 * will catch the Exception and return an empty String. This can be overruled by setting the
 * {@link cascading.management.annotation.URISanitizer#FAILURE_MODE_PASS_THROUGH} System property to {@code true},
 * which will cause the actual value being returned. <b>Note</b> that this might leak sensitive information to the
 * {@link cascading.management.DocumentService}.
 */
public class URISanitizer implements Sanitizer
  {
  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger( URISanitizer.class );

  /**
   * System property for listing URI parameters to be filtered out (usernames, passwords etc.)
   * <p>
   * Value cases are ignored, thus {@code UserName} will be equivalent to {@code username}.
   */
  public static final String PARAMETER_FILTER_PROPERTY = "cascading.management.annotation.urisanitizer.parameternames";

  /** System property to allow values to pass through a parse exception. */
  public static final String FAILURE_MODE_PASS_THROUGH = "cascading.management.annotation.urisanitizer.failurepassthrough";

  private Set<String> parametersToFilter;

  public URISanitizer()
    {
    String parameterProperty = System.getProperty( PARAMETER_FILTER_PROPERTY );

    if( Util.isEmpty( parameterProperty ) )
      {
      parametersToFilter = Collections.emptySet();
      }
    else
      {
      // treat "UserName" equal to "username"
      parametersToFilter = new TreeSet<String>( String.CASE_INSENSITIVE_ORDER );

      String[] parameterNames = parameterProperty.split( "," );

      for( String parameterName : parameterNames )
        {
        if( parameterName != null )
          parameterName = parameterName.trim();

        if( !Util.isEmpty( parameterName ) )
          parametersToFilter.add( parameterName );
        }
      }
    }

  @Override
  public String apply( Visibility visibility, Object value )
    {
    if( value == null )
      return null;

    URI uri;

    if( value instanceof URI )
      {
      uri = (URI) value;
      }
    else
      {
      try
        {
        uri = URI.create( encode( value.toString() ) );
        }
      catch( IllegalArgumentException exception )
        {
        LOG.warn( "failed to parse uri: {}, message: {}", value, exception.getMessage() );
        LOG.debug( "failed to parse uri: {}", value, exception );

        if( Boolean.parseBoolean( System.getProperty( FAILURE_MODE_PASS_THROUGH ) ) )
          {
          LOG.warn( "ignoring uri sanitizer failures, returning unsanitized value, property '{}' set to true", FAILURE_MODE_PASS_THROUGH );
          return value.toString();
          }

        // return an empty string, to avoid the leakage of sensitive information.
        LOG.info( "set property: '{}', to true to return unsanitized value, returning empty string", FAILURE_MODE_PASS_THROUGH );
        return "";
        }
      }

    if( uri.isOpaque() )
      {
      switch( visibility )
        {
        case PRIVATE:
          return value.toString();
        case PROTECTED:
        case PUBLIC:
          return uri.getScheme() + ":";
        }
      }

    StringBuilder buffer = new StringBuilder();

    if( uri.getPath() != null ) // can happen according to the javadoc
      buffer.append( uri.getPath() );

    if( ( visibility == Visibility.PROTECTED || visibility == Visibility.PRIVATE ) && uri.getQuery() != null )
      buffer.append( "?" ).append( sanitizeQuery( uri.getQuery() ) );

    if( visibility == Visibility.PRIVATE )
      {
      String currentString = buffer.toString(); // preserve before creating a new instance
      buffer = new StringBuilder();

      if( uri.getScheme() != null )
        buffer.append( uri.getScheme() ).append( "://" );

      if( uri.getAuthority() != null )
        buffer.append( uri.getAuthority() );

      buffer.append( currentString );
      }

    return buffer.toString();
    }

  private String encode( String input )
    {
    String[] parts = input.split( "://", 2 );
    String protocol = "";
    String rest;

    if( parts.length == 2 )
      protocol = parts[ 0 ];

    rest = parts[ parts.length - 1 ];

    rest = rest.replaceAll( "\\[", "%5B" );
    rest = rest.replaceAll( "\\]", "%5D" );
    rest = rest.replaceAll( "\\{", "%7B" );
    rest = rest.replaceAll( "\\}", "%7D" );
    rest = rest.replaceAll( "\\\\", "/" );
    rest = rest.replaceAll( ";", "%3B" );
    rest = rest.replaceAll( ",", "%2C" );

    StringBuilder builder = new StringBuilder();

    if( !protocol.isEmpty() )
      builder.append( protocol ).append( "://" );

    builder.append( rest );

    return builder.toString();
    }

  private String sanitizeQuery( String query )
    {
    StringBuilder buffer = new StringBuilder();
    String[] parts = query.split( "&" );

    for( String part : parts )
      {
      String[] keyValuePair = part.split( "=" );
      String key = keyValuePair[ 0 ];

      if( parametersToFilter.contains( key ) )
        continue;

      buffer.append( part ).append( "&" );
      }

    return buffer.toString();
    }
  }
