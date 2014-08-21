/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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
 * <ul>
 * <li>PUBLIC: Only return the path of the URI</li>
 * <li>PROTECTED: Same as PUBLIC + query parameters</li>
 * <li>PRIVATE: Same as PROTECTED + URI scheme and authority (host/port)</li>
 * </ul>
 * <p/>
 * <p>Parameters containing sensitive information like user-names, passwords, API-keys etc. can be filtered out by setting
 * the {@link cascading.management.annotation.URISanitizer#PARAMETER_FILTER_PROPERTY} System property to a comma separated
 * list of names that should never show up in the {@link cascading.management.DocumentService}. Some systems may use
 * non-standard URIs, which cannot be parsed by {@link java.net.URI}.</p>
 * <p/>
 * <p>If the sanitizer encounters one of those URIs it
 * will catch the Exception and return an empty String. This can be overruled by setting the
 * {@link cascading.management.annotation.URISanitizer#FAILURE_MODE_PASS_THROUGH} System property to <code>true</code>,
 * which will cause the actual value being returned. <b>Note</b> that this might leak sensitive information to the
 * {@link cascading.management.DocumentService}.</p>
 */
public class URISanitizer implements Sanitizer
  {
  /**
   * Logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger( URISanitizer.class );

  /**
   * System property for listing URI parameters to be filtered out (usernames, passwords etc.)
   * <p/>
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
        uri = URI.create( value.toString() );
        }
      catch( IllegalArgumentException exception )
        {
        LOG.warn( "failed to parse uri: {}", value, exception );

        if( Boolean.parseBoolean( System.getProperty( FAILURE_MODE_PASS_THROUGH ) ) )
          {
          LOG.warn( "ignoring failures, returning raw value" );
          return value.toString();
          }

        // return an empty string, to avoid the leakage of sensitive information.
        return "";
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
