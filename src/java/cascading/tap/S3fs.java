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

package cascading.tap;

import java.io.IOException;
import java.net.URI;

import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;

/**
 * Class S3fs is a {@link Tap} class that provides access to the Amazon S3 storage system.
 * <p/>
 * An Amazon AWS id, secret key, and a S3 bucket name are required.
 */
public class S3fs extends Hfs
  {
  /** Field S3FS_ID is the property key for the S3 id */
  public static final String S3FS_ID = "s3fs.id";
  /** Field S3FS_SECRET is the property key for the S3 secret */
  public static final String S3FS_SECRET = "s3fs.secret";
  /** Field S3FS_BUCKET is the property key for the S3 bucket */
  public static final String S3FS_BUCKET = "s3fs.bucket";

  /** Field uri */
  private URI uri;

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param sourceFields of type Fields
   * @param uri          of type URI
   */
  public S3fs( Fields sourceFields, URI uri )
    {
    super( sourceFields, uri.getPath() );

    if( !uri.getScheme().equalsIgnoreCase( "s3" ) )
      throw new IllegalArgumentException( "uri must use the s3 scheme" );

    this.uri = URI.create( uri.getScheme() + "://" + uri.getAuthority() );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param sourceFields of type Fields
   * @param uri of type URI
   * @param deleteOnSinkInit of type boolean
   */
  public S3fs( Fields sourceFields, URI uri, boolean deleteOnSinkInit )
    {
    super( sourceFields, uri.getPath(), deleteOnSinkInit );

    if( !uri.getScheme().equalsIgnoreCase( "s3" ) )
      throw new IllegalArgumentException( "uri must use the s3 scheme" );

    this.uri = URI.create( uri.getScheme() + "://" + uri.getAuthority() );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param sourceFields of type Fields
   * @param stringPath   of type String
   */
  public S3fs( Fields sourceFields, String stringPath )
    {
    super( sourceFields, stringPath );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param sourceFields of type Fields
   * @param id           of type String
   * @param secret       of type String
   * @param bucket       of type String
   * @param stringPath   of type String
   */
  public S3fs( Fields sourceFields, String id, String secret, String bucket, String stringPath )
    {
    super( sourceFields, stringPath );
    this.uri = makeURI( id, secret, bucket );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param scheme     of type Scheme
   * @param id         of type String
   * @param secret     of type String
   * @param bucket     of type String
   * @param stringPath of type String
   */
  public S3fs( Scheme scheme, String id, String secret, String bucket, String stringPath )
    {
    super( scheme, stringPath );
    this.uri = makeURI( id, secret, bucket );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param scheme of type Scheme
   * @param uri of type URI
   */
  public S3fs( Scheme scheme, URI uri )
    {
    super( scheme, uri.getPath() );

    if( !uri.getScheme().equalsIgnoreCase( "s3" ) )
      throw new IllegalArgumentException( "uri must use the s3 scheme" );

    this.uri = URI.create( uri.getScheme() + "://" + uri.getAuthority() );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param scheme     of type Scheme
   * @param stringPath of type String
   */
  public S3fs( Scheme scheme, String stringPath )
    {
    super( scheme, stringPath );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param scheme       of type Scheme
   * @param id           of type String
   * @param secret       of type String
   * @param bucket       of type String
   * @param stringPath   of type String
   * @param deleteOnInit of type boolean
   */
  public S3fs( Scheme scheme, String id, String secret, String bucket, String stringPath, boolean deleteOnInit )
    {
    super( scheme, stringPath, deleteOnInit );
    this.uri = makeURI( id, secret, bucket );
    }

  /**
   * Constructor S3fs creates a new S3fs instance.
   *
   * @param scheme       of type Scheme
   * @param stringPath   of type String
   * @param deleteOnInit of type boolean
   */
  public S3fs( Scheme scheme, String stringPath, boolean deleteOnInit )
    {
    super( scheme, stringPath, deleteOnInit );
    }

  protected FileSystem getFileSystem( JobConf conf ) throws IOException
    {
    if( uri != null )
      return FileSystem.get( uri, conf );
    else
      return FileSystem.get( getUri( conf ), conf ); // since uri is dervied, we don't cache the value
    }

  private URI getUri( JobConf conf )
    {
    return makeURI( conf.get( S3FS_ID ), conf.get( S3FS_SECRET ), conf.get( S3FS_BUCKET ) );
    }

  private URI makeURI( String id, String secret, String bucket )
    {
    if( id == null || id.length() == 0 )
      throw new IllegalArgumentException( "id may not be null" );

    if( secret == null || secret.length() == 0 )
      throw new IllegalArgumentException( "secret may not be null" );

    if( bucket == null || bucket.length() == 0 )
      throw new IllegalArgumentException( "bucket may not be null" );

    return URI.create( "s3://" + id + ":" + secret + "@" + bucket );
    }
  }