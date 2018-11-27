/*
 * Copyright (c) 2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.local.tap.aws.s3;

import java.util.Properties;

import cascading.property.Props;

/**
 * Class S3TapProps provides S3 specific properties for overriding the AWS client
 * endpoint and region.
 */
public class S3TapProps extends Props
  {
  /** Field S3_ENDPOINT */
  public static final String S3_ENDPOINT = "cascading.tap.aws.s3.endpoint";
  /** Field S3_REGION */
  public static final String S3_REGION = "cascading.tap.aws.s3.region";
  /** Field S3_PATH_STYLE_ACCESS */
  public static final String S3_PATH_STYLE_ACCESS = "cascading.tap.aws.s3.path_style_access";
  /** Field S3_PROXY_HOST */
  public static final String S3_PROXY_HOST = "cascading.tap.aws.s3.proxy.host";
  /** Field S3_PROXY_PORT */
  public static final String S3_PROXY_PORT = "cascading.tap.aws.s3.proxy.port";

  /** Field endpoint */
  String endpoint;
  /** Field region */
  String region;
  /** pathStyleAccess */
  boolean pathStyleAccess = false;
  /** Field proxyHost */
  String proxyHost;
  /** Field proxyPort */
  int proxyPort;

  /**
   * Constructor S3TapProps creates a new S3TapProps instance.
   */
  public S3TapProps()
    {
    }

  /**
   * Method getEndpoint returns the endpoint of this S3TapProps object.
   *
   * @return the endpoint (type String) of this S3TapProps object.
   */
  public String getEndpoint()
    {
    return endpoint;
    }

  /**
   * Method setEndpoint sets the endpoint of this S3TapProps object.
   *
   * @param endpoint the endpoint of this S3TapProps object.
   * @return S3TapProps
   */
  public S3TapProps setEndpoint( String endpoint )
    {
    this.endpoint = endpoint;

    return this;
    }

  /**
   * Method getRegion returns the region of this S3TapProps object.
   *
   * @return the region (type String) of this S3TapProps object.
   */
  public String getRegion()
    {
    return region;
    }

  /**
   * Method setRegion sets the region of this S3TapProps object.
   *
   * @param region the region of this S3TapProps object.
   * @return S3TapProps
   */
  public S3TapProps setRegion( String region )
    {
    this.region = region;

    return this;
    }

  /**
   * Method isPathStyleAccess returns true if the underlying S3 client should use
   * path style access to the S3 host.
   *
   * @return true if path style access should be used
   */
  public boolean isPathStyleAccess()
    {
    return pathStyleAccess;
    }

  /**
   * Method setPathStyleAccess should be called if the underlying S3 client should
   * use path style access to reach the target S3 host.
   *
   * @param pathStyleAccess true if path style access should be used.
   * @return S3TapProps
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html">Virtual Hosting</a>
   */
  public S3TapProps setPathStyleAccess( boolean pathStyleAccess )
    {
    this.pathStyleAccess = pathStyleAccess;

    return this;
    }

  /**
   * Method getProxyHost returns the optional S3 proxy host of this S3TapProps object.
   *
   * @return the S3 proxy host (type String) of this S3TapProps object.
   */
  public String getProxyHost()
    {
    return proxyHost;
    }

  /**
   * Method setProxyHost sets the optional proxy host this S3TapProps object will connect through.
   *
   * @param proxyHost the proxy host this S3TapProps object will connect through .
   * @return S3TapProps
   */
  public S3TapProps setProxyHost( String proxyHost )
    {
    this.proxyHost = proxyHost;

    return this;
    }

  /**
   * Method getProxyPort returns the optional S3 proxy port this S3TapProps object will connect through
   *
   * @return the S3 proxy port (type int) this S3TapProps object will connect through.
   */
  public int getProxyPort()
    {
    return proxyPort;
    }

  /**
   * Method setProxyPort sets the optional proxy port this S3TapProps object will connect through.
   *
   * @param proxyPort the proxy host this S3TapProps object will connect through .
   * @return S3TapProps
   */
  public S3TapProps setProxyPort( int proxyPort )
    {
    this.proxyPort = proxyPort;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    if( endpoint != null )
      properties.setProperty( S3_ENDPOINT, endpoint );

    if( region != null )
      properties.setProperty( S3_REGION, region );

    if( pathStyleAccess )
      properties.setProperty( S3_PATH_STYLE_ACCESS, "true" );

    if( proxyHost != null )
      properties.setProperty( S3_PROXY_HOST, proxyHost );

    if( proxyPort > 0 )
      properties.setProperty( S3_PROXY_PORT, String.valueOf( proxyPort ) );
    }
  }
