/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
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
import java.net.URI;

import cascading.CascadingException;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/** Class S3Util encapsulates calls to the JetS3t API. */
public class S3Util
  {
  public enum Request
    {
      DETAILS, OBJECT, CREATE
    }

  /**
   * Parses the userinfo username and password out of the given URI, and retuns the embedded, or default, AWS access and secret keys.
   * <p/>
   * This code will handle underscores in bucket names.
   *
   * @param uri
   * @param defaultAccessKey
   * @param defaultSecretAccessKey
   * @return a String[] with accessKey and secretKey
   */
  public static String[] parseAWSUri( URI uri, String defaultAccessKey, String defaultSecretAccessKey )
    {
    String accessKey = null;
    String secretAccessKey = null;
    String userInfo = uri.getUserInfo();

    // special handling for underscores in bucket names
    if( userInfo == null )
      {
      String authority = uri.getAuthority();
      String split[] = authority.split( "[:@]" );

      if( split.length >= 2 )
        userInfo = split[ 0 ] + ":" + split[ 1 ];
      }

    if( userInfo != null )
      {
      int index = userInfo.indexOf( ':' );

      if( index != -1 )
        {
        accessKey = userInfo.substring( 0, index );
        secretAccessKey = userInfo.substring( index + 1 );
        }
      else
        {
        accessKey = userInfo;
        }
      }

    if( accessKey == null )
      accessKey = defaultAccessKey;

    if( secretAccessKey == null )
      secretAccessKey = defaultSecretAccessKey;

    if( accessKey == null && secretAccessKey == null )
      throw new IllegalArgumentException( "AWS Access Key ID and Secret Access Key must be specified as the username or password in the given URI" );
    else if( accessKey == null )
      throw new IllegalArgumentException( "AWS Access Key ID must be specified as the username of the given URI" );
    else if( secretAccessKey == null )
      throw new IllegalArgumentException( "AWS Secret Access Key must be specified as the password of the given URI" );

    return new String[]{accessKey, secretAccessKey};
    }

  public static RestS3Service getS3Service( URI uri )
    {
    return getS3Service( uri, null, null );
    }

  public static RestS3Service getS3Service( URI uri, String defaultAccessKey, String defaultSecretAccessKey )
    {
    try
      {
      String[] aws = parseAWSUri( uri, defaultAccessKey, defaultSecretAccessKey );

      return new RestS3Service( new AWSCredentials( aws[ 0 ], aws[ 1 ] ) );
      }
    catch( S3ServiceException exception )
      {
      if( exception.getCause() instanceof IOException )
        throw new CascadingException( exception.getCause() );

      throw new CascadingException( exception );
      }
    }

  public static S3Bucket getS3Bucket( URI uri )
    {
    String bucketName = uri.getAuthority();

    // handling for underscore in bucket name
    if( bucketName.contains( "@" ) )
      bucketName = bucketName.split( "@" )[ 1 ];

    return new S3Bucket( bucketName );
    }

  public static boolean deleteObject( S3Service s3Service, S3Bucket s3Bucket, Path path ) throws IOException
    {
    S3Object object = getObject( s3Service, s3Bucket, path, Request.DETAILS );

    if( object == null )
      return true;

    try
      {
      s3Service.deleteObject( s3Bucket, object.getKey() );
      }
    catch( S3ServiceException exception )
      {
      return false;
      }

    return true;
    }

  /**
   * @param s3Service
   * @param s3Bucket
   * @param path
   * @param type
   * @return null if the S3 service returns a 404
   * @throws IOException thrown if there is an error communicating to S3
   */
  public static S3Object getObject( S3Service s3Service, S3Bucket s3Bucket, Path path, Request type ) throws IOException
    {
    try
      {
      URI uri = path.toUri();
      String keyName = uri.getPath().substring( 1 );

      if( type == Request.CREATE )
        return new S3Object( s3Bucket, keyName );
      else if( type == Request.DETAILS )
        return s3Service.getObjectDetails( s3Bucket, keyName );
      else if( type == Request.OBJECT )
        return s3Service.getObject( s3Bucket, keyName );
      else
        throw new IllegalArgumentException( "unrecognized request type: " + type );
      }
    catch( S3ServiceException exception )
      {
      if( exception.getMessage().contains( "404" ) )
        return null;

      IOException ioException = new IOException( "could not get object: " + path );

      ioException.initCause( exception );

      throw ioException;
      }
    }

  public static void putObject( RestS3Service s3Service, S3Bucket bucket, S3Object object ) throws IOException
    {
    try
      {
      s3Service.putObject( bucket, object );
      }
    catch( S3ServiceException exception )
      {
      if( exception.getCause() instanceof IOException )
        throw (IOException) exception.getCause();

      throw new IOException( "could not store object: " + bucket.getName() + "/" + object.getKey() + " " + exception.getMessage() );
      }
    }

  public static InputStream getObjectInputStream( S3Object object ) throws IOException
    {
    try
      {
      return object.getDataInputStream();
      }
    catch( S3ServiceException exception )
      {
      IOException ioException = new IOException( "could get object inputstream: " + object.getKey() );

      ioException.initCause( exception );

      throw ioException;
      }
    }


  }
