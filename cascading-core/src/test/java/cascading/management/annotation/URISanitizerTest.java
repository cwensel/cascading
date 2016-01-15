/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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
import java.net.URISyntaxException;

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for uriSanitizer.
 */
public class URISanitizerTest
  {

  @After
  public void tearDown()
    {
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "" );
    System.setProperty( URISanitizer.FAILURE_MODE_PASS_THROUGH, "false" );
    }

  @Test
  public void testNull()
    {
    URISanitizer sanitizer = new URISanitizer();
    assertNull( sanitizer.apply( Visibility.PUBLIC, null ) );
    }

  @Test
  public void testSyntaxException()
    {
    URISanitizer sanitizer = new URISanitizer();
    String malformed = " http://example.com";
    assertEquals( "", sanitizer.apply( Visibility.PUBLIC, malformed ) );
    }

  @Test
  public void testSyntaxExceptionPassThrough()
    {
    System.setProperty( URISanitizer.FAILURE_MODE_PASS_THROUGH, "true" );
    URISanitizer sanitizer = new URISanitizer();
    String malformed = " http://example.com";
    assertEquals( malformed, sanitizer.apply( Visibility.PUBLIC, malformed ) );
    }

  @Test
  public void testURIPublicSanitizationWithString()
    {
    String uri = "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PUBLIC, uri );
    assertEquals( "/docs/resource1.html", result );
    }

  @Test
  public void testURIProtectedSanitizationWithMixedCaseString()
    {
    // tests for case insensitivity
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "user,paSSword" );
    String uri = "http://www.example.com:8080/docs/resource1.html?User=foo&password=secret&action=do";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/docs/resource1.html?action=do&", result );
    }

  @Test
  public void testURIProtectedSanitizationWithString()
    {
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "user,password" );
    String uri = "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/docs/resource1.html?action=do&", result );
    }

  @Test
  public void testURIPrivateSanitizationWithString()
    {
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "user,password" );
    String uri = "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PRIVATE, uri );
    assertEquals( "http://www.example.com:8080/docs/resource1.html?action=do&", result );
    }

  @Test
  public void testURIPrivateSanitizationNoScheme()
    {
    String uri = "data/stuff";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PRIVATE, uri );
    assertEquals( uri, result );
    }

  @Test
  public void testURIPublicSanitizationWithURI() throws IllegalArgumentException
    {
    URI uri = URI.create( "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do" );
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PUBLIC, uri );
    assertEquals( "/docs/resource1.html", result );
    }

  @Test
  public void testURIProtectedSanitizationWithURI() throws URISyntaxException
    {
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "user,password" );
    URI uri = URI.create( "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do" );
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/docs/resource1.html?action=do&", result );
    }

  @Test
  public void testURIPrivateSanitizationWithURI() throws URISyntaxException
    {
    System.setProperty( URISanitizer.PARAMETER_FILTER_PROPERTY, "user,password" );
    URI uri = URI.create( "http://www.example.com:8080/docs/resource1.html?user=foo&password=secret&action=do" );
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PRIVATE, uri );
    assertEquals( "http://www.example.com:8080/docs/resource1.html?action=do&", result );
    }

  @Test
  public void testHDFSPublic()
    {
    String uri = "hdfs://hadoop42.example.com:8020/some/dataset";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PUBLIC, uri );
    assertEquals( "/some/dataset", result );
    }

  @Test
  public void testHDFSProtected()
    {
    String uri = "hdfs://hadoop42.example.com:8020/some/dataset";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/some/dataset", result );
    }

  @Test
  public void testHDFSPrivate()
    {
    String uri = "hdfs://hadoop42.example.com:8020/some/dataset/";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PRIVATE, uri );
    assertEquals( uri, result );
    }

  @Test
  public void testS3NGlob()
    {
    String uri = "s3n://some-bucket/2014/12/2[5-9]/*";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/2014/12/2[5-9]/*", result );
    }

  @Test
  public void testOpaqueURI()
    {
    String uri = "memory:driven.agent.driven.spark.SkewExampleTest.testSubmit(SkewExampleTest.scala:36)";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PRIVATE, uri );
    assertEquals( "memory:driven.agent.driven.spark.SkewExampleTest.testSubmit(SkewExampleTest.scala:36)", result );

    result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "memory:", result );

    result = sanitizer.apply( Visibility.PUBLIC, uri );
    assertEquals( "memory:", result );
    }

  @Test
  public void testS3NGlob2()
    {
    String uri = "s3n://host/path/conversion_date={2015-12-23,2015-12-22}";
    URISanitizer sanitizer = new URISanitizer();
    String result = sanitizer.apply( Visibility.PROTECTED, uri );
    assertEquals( "/path/conversion_date={2015-12-23,2015-12-22}", result );
    }
  }
