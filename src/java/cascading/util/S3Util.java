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

import cascading.operation.OperationException;
import org.apache.hadoop.fs.Path;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

/**
 *
 */
public class S3Util
  {
  public enum Request
    {
      DETAILS, OBJECT
    }

  public static RestS3Service getS3Service( URI uri )
    {
    try
      {
      String accessKey = null;
      String secretAccessKey = null;
      String userInfo = uri.getUserInfo();

      // special handling for underscores in bucket names
      if( userInfo == null )
        {
        String authority = uri.getAuthority();

        String split[] = authority.split( "[:@]" );

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

      if( accessKey == null && secretAccessKey == null )
        throw new IllegalArgumentException(
          "AWS Access Key ID and Secret Access Key must be specified as the username or password (respectively) of a s3 URL" );
      else if( accessKey == null )
        throw new IllegalArgumentException( "AWS Access Key ID must be specified as the username of a s3 URL" );
      else if( secretAccessKey == null )
        throw new IllegalArgumentException( "AWS Secret Access Key must be specified as the password of a s3 URL" );

      return new RestS3Service( new AWSCredentials( accessKey, secretAccessKey ) );
      }
    catch( S3ServiceException exception )
      {
      if( exception.getCause() instanceof IOException )
        throw new OperationException( exception.getCause() );

      throw new OperationException( exception );
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

  public static S3Object getObject( S3Service s3Service, S3Bucket s3Bucket, Path path, Request type ) throws IOException
    {
    try
      {
      URI uri = path.toUri();
      String keyName = uri.getPath().substring( 1 );

      if( type == Request.DETAILS )
        return s3Service.getObjectDetails( s3Bucket, keyName );
      else if( type == Request.OBJECT )
        return s3Service.getObject( s3Bucket, keyName );
      else
        throw new IllegalArgumentException( "unrecognized request type: " + type );
      }
    catch( S3ServiceException exception )
      {
      IOException ioException = new IOException( "could not get object: " + path );

      ioException.initCause( exception );

      throw ioException;
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
