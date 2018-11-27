/*
 * Copyright (c) 2017-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import cascading.flow.FlowProcess;
import cascading.property.PropertyUtil;
import cascading.scheme.FileFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.PartitionTap;
import cascading.tap.type.FileType;
import cascading.tap.type.TapWith;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.CloseableIterator;
import cascading.util.Util;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.util.Util.isEmpty;

/**
 * Class S3Tap is a Cascading local-mode {@link Tap} providing read and write access to data stored in Amazon S3 buckets.
 * <p>
 * This Tap is not intended to be used with any of the other Cascading planners unless they specify they are local-mode
 * compatible.
 * <p>
 * S3Tap can read a single key, all objects underneath a key-prefix, or all objects under a key-prefix that match
 * a given globbing pattern.
 * <p>
 * See the various constructors for the available access parametrizations. Of note are the constructors that take
 * a {@link URI} instance. The URI should be in the following format:
 * {@code s3://[bucket]/<key|key-prefix><?glob>}
 * <p>
 * Where bucket is the only required value. The key references a single object, the key-prefix is used to access
 * a set of objects with a common prefix value. The glob value is use to further narrow the resulting object set.
 * <p>
 * The globbing pattern is specified by the {@link java.nio.file.FileSystem#getPathMatcher} method.
 * <p>
 * This Tap was designed to allow applications to effectively poll an S3 bucket for new keys to be processed.
 * <p>
 * When used with the {@link S3FileCheckpointer} class, a map of keys last consumed by each bucket will be tracked
 * on disk, with the map surviving JVM restarts allowing for applications to exit and restart safely without
 * retrieving duplicate data.
 * <p>
 * The {@link S3Checkpointer#commit()} method is only called during a graceful shutdown of the Flow or JVM, but every
 * consumed key is passed to the S3Checkpointer, so custom implementations can choose to persist the key more
 * frequently.
 * <p>
 * AWS Credentials are handled by {@link com.amazonaws.auth.DefaultAWSCredentialsProviderChain}.
 */
public class S3Tap extends Tap<Properties, InputStream, OutputStream> implements FileType<Properties>, TapWith<Properties, InputStream, OutputStream>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( S3Tap.class );

  /** Field SEQUENCE_TOKEN */
  public static final String SEQUENCE_TOKEN = "{sequence}";
  /** Field MIME_DIRECTORY */
  public static final String MIME_DIRECTORY = "application/x-directory";
  /** Field DEFAULT_DELIMITER */
  public static final String DEFAULT_DELIMITER = "/";

  /** Field s3Client */
  AmazonS3 s3Client = null;
  /** Field bucketName */
  String bucketName = null;
  /** Field key */
  String key = null;
  /** Field filter */
  Predicate<String> filter;
  /** Field delimiter */
  String delimiter = DEFAULT_DELIMITER;
  /** Field checkpointer */
  S3Checkpointer checkpointer;

  private transient ObjectMetadata objectMetadata;

  /**
   * Method makeURI creates a new S3 URI from the given parameters.
   *
   * @param bucketName the S3 bucket name
   * @param keyPrefix  the S3 object key or key-prefix
   * @return an URI instance
   */
  public static URI makeURI( String bucketName, String keyPrefix )
    {
    return makeURI( bucketName, keyPrefix, null );
    }

  /**
   * Method makeURI creates a new S3 URI from the given parameters.
   *
   * @param bucketName the S3 bucket name
   * @param keyPrefix  the S3 object key or key-prefix
   * @param glob       the globbing pattern to apply to the keys
   * @return an URI instance
   */
  public static URI makeURI( String bucketName, String keyPrefix, String glob )
    {
    if( bucketName == null )
      throw new IllegalArgumentException( "bucketName may not be null" );

    try
      {
      if( keyPrefix == null )
        keyPrefix = "/";
      else if( !keyPrefix.startsWith( "/" ) )
        keyPrefix = "/" + keyPrefix;

      return new URI( "s3", bucketName, keyPrefix, glob, null );
      }
    catch( URISyntaxException exception )
      {
      throw new IllegalArgumentException( exception.getMessage(), exception );
      }
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName )
    {
    this( scheme, bucketName, null, null, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key )
    {
    this( scheme, bucketName, key, DEFAULT_DELIMITER, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, String delimiter )
    {
    this( scheme, null, null, bucketName, key, delimiter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, Predicate<String> filter )
    {
    this( scheme, bucketName, null, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, Predicate<String> filter )
    {
    this( scheme, bucketName, key, DEFAULT_DELIMITER, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, String delimiter, Predicate<String> filter )
    {
    this( scheme, null, null, bucketName, key, delimiter, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName )
    {
    this( scheme, s3Client, bucketName, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key )
    {
    this( scheme, s3Client, bucketName, key, DEFAULT_DELIMITER, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key, String delimiter )
    {
    this( scheme, s3Client, bucketName, key, delimiter, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key, String delimiter, Predicate<String> filter )
    {
    this( scheme, s3Client, null, bucketName, key, delimiter, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName )
    {
    this( scheme, checkpointer, bucketName, null, null, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key )
    {
    this( scheme, checkpointer, bucketName, key, DEFAULT_DELIMITER, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, String delimiter )
    {
    this( scheme, null, checkpointer, bucketName, key, delimiter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param filter       of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, Predicate<String> filter )
    {
    this( scheme, checkpointer, bucketName, null, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param filter       of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, Predicate<String> filter )
    {
    this( scheme, checkpointer, bucketName, key, DEFAULT_DELIMITER, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param filter       of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, Predicate<String> filter )
    {
    this( scheme, null, checkpointer, bucketName, key, delimiter, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName )
    {
    this( scheme, s3Client, checkpointer, bucketName, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key )
    {
    this( scheme, s3Client, checkpointer, bucketName, key, DEFAULT_DELIMITER, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key, String delimiter )
    {
    this( scheme, s3Client, checkpointer, bucketName, key, delimiter, null, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param filter       of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, Predicate<String> filter )
    {
    this( scheme, s3Client, checkpointer, bucketName, key, delimiter, filter, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, SinkMode sinkMode )
    {
    this( scheme, bucketName, null, null, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, SinkMode sinkMode )
    {
    this( scheme, bucketName, key, DEFAULT_DELIMITER );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, String delimiter, SinkMode sinkMode )
    {
    this( scheme, null, null, bucketName, key, delimiter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, bucketName, null, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param filter     of Predicate
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, bucketName, key, DEFAULT_DELIMITER, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   * @param filter     of Predicate
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, String bucketName, String key, String delimiter, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, null, null, bucketName, key, delimiter, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, SinkMode sinkMode )
    {
    this( scheme, s3Client, bucketName, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key, SinkMode sinkMode )
    {
    this( scheme, s3Client, bucketName, key, DEFAULT_DELIMITER, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key, String delimiter, SinkMode sinkMode )
    {
    this( scheme, s3Client, bucketName, key, delimiter, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param bucketName of String
   * @param key        of String
   * @param delimiter  of String
   * @param filter     of Predicate
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, String bucketName, String key, String delimiter, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, s3Client, null, bucketName, key, delimiter, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, SinkMode sinkMode )
    {
    this( scheme, checkpointer, bucketName, null, null, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, SinkMode sinkMode )
    {
    this( scheme, checkpointer, bucketName, key, DEFAULT_DELIMITER, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, SinkMode sinkMode )
    {
    this( scheme, null, checkpointer, bucketName, key, delimiter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param filter       of Predicate
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, checkpointer, bucketName, null, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param filter       of Predicate
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, checkpointer, bucketName, key, DEFAULT_DELIMITER, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param filter       of Predicate
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, Predicate<String> filter, SinkMode sinkMode )
    {
    this( scheme, null, checkpointer, bucketName, key, delimiter, filter, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, SinkMode sinkMode )
    {
    this( scheme, s3Client, checkpointer, bucketName, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key, SinkMode sinkMode )
    {
    this( scheme, s3Client, checkpointer, bucketName, key, DEFAULT_DELIMITER, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, SinkMode sinkMode )
    {
    this( scheme, s3Client, checkpointer, bucketName, key, delimiter, null, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param bucketName   of String
   * @param key          of String
   * @param delimiter    of String
   * @param filter       of Predicate
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, String bucketName, String key, String delimiter, Predicate<String> filter, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.s3Client = s3Client;
    this.checkpointer = checkpointer;
    this.bucketName = bucketName;

    if( isEmpty( this.bucketName ) )
      throw new IllegalArgumentException( "bucket name may not be null or empty" );

    this.key = key;
    this.delimiter = delimiter;
    this.filter = filter;
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param identifier of URI
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, URI identifier )
    {
    this( scheme, null, null, identifier, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param identifier of URI
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, URI identifier )
    {
    this( scheme, s3Client, null, identifier, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param identifier   of URI
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, URI identifier )
    {
    this( scheme, null, checkpointer, identifier, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param identifier   of URI
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, URI identifier )
    {
    this( scheme, s3Client, checkpointer, identifier, SinkMode.KEEP );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param identifier of URI
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, URI identifier, SinkMode sinkMode )
    {
    this( scheme, null, null, identifier, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme     of Scheme
   * @param s3Client   of AmazonS3
   * @param identifier of URI
   * @param sinkMode   of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, URI identifier, SinkMode sinkMode )
    {
    this( scheme, s3Client, null, identifier, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param checkpointer of S3Checkpointer
   * @param identifier   of URI
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, S3Checkpointer checkpointer, URI identifier, SinkMode sinkMode )
    {
    this( scheme, null, checkpointer, identifier, sinkMode );
    }

  /**
   * Constructor S3Tap creates a new S3Tap instance.
   *
   * @param scheme       of Scheme
   * @param s3Client     of AmazonS3
   * @param checkpointer of S3Checkpointer
   * @param identifier   of URI
   * @param sinkMode     of SinkMode
   */
  public S3Tap( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme, AmazonS3 s3Client, S3Checkpointer checkpointer, URI identifier, SinkMode sinkMode )
    {
    super( scheme, sinkMode );
    this.s3Client = s3Client;
    this.checkpointer = checkpointer;

    if( identifier == null )
      throw new IllegalArgumentException( "identifier may not be null" );

    if( !identifier.getScheme().equalsIgnoreCase( "s3" ) )
      throw new IllegalArgumentException( "identifier does not have s3 scheme" );

    this.bucketName = getBucketNameFor( identifier );

    if( isEmpty( this.bucketName ) )
      throw new IllegalArgumentException( "bucket name may not be null or empty" + identifier );

    this.key = cleanKey( identifier );

    if( identifier.getQuery() != null )
      filter = globPredicate( identifier.getQuery() );
    }

  protected String getBucketNameFor( URI identifier )
    {
    String authority = identifier.getAuthority();

    if( isEmpty( authority ) )
      throw new IllegalArgumentException( "identifier must have an authority: " + identifier );

    int pos = authority.indexOf( '@' );

    if( pos != -1 )
      return authority.substring( pos + 1 );

    return authority;
    }

  private static Predicate<String> globPredicate( String glob )
    {
    String regex = getRegexForGlob( glob );
    Pattern pattern = Pattern.compile( regex );

    return string -> pattern.matcher( string ).matches();
    }

  private static String getRegexForGlob( String glob )
    {
    return (String) Util.invokeStaticMethod(
      "sun.nio.fs.Globs",
      "toUnixRegexPattern",
      new Object[]{glob},
      new Class[]{String.class}
    );
    }

  @Override
  public TapWith<Properties, InputStream, OutputStream> withScheme( Scheme<Properties, InputStream, OutputStream, ?, ?> scheme )
    {
    // don't lazily create s3Client
    return new S3Tap( scheme, s3Client, getBucketName(), getKey(), getDelimiter(), getFilter(), getSinkMode() );
    }

  @Override
  public TapWith<Properties, InputStream, OutputStream> withChildIdentifier( String identifier )
    {
    URI uri;

    if( identifier.startsWith( "s3://" ) )
      uri = URI.create( identifier );
    else if( identifier.startsWith( getBucketName() ) )
      uri = makeURI( identifier, null );
    else
      uri = makeURI( getBucketName(), getKey() + ( identifier.startsWith( delimiter ) ? identifier : delimiter + identifier ) );

    // don't lazily create s3Client
    return new S3Tap( getScheme(), s3Client, uri, getSinkMode() );
    }

  @Override
  public TapWith<Properties, InputStream, OutputStream> withSinkMode( SinkMode sinkMode )
    {
    // don't lazily create s3Client
    return new S3Tap( getScheme(), s3Client, getBucketName(), getKey(), getDelimiter(), getFilter(), sinkMode );
    }

  protected String cleanKey( URI identifier )
    {
    String path = identifier.getPath();

    if( path.startsWith( "/" ) )
      path = path.substring( 1 );

    return path;
    }

  protected AmazonS3 getS3Client( Properties properties )
    {
    // return provided client
    if( s3Client != null )
      return s3Client;

    AmazonS3ClientBuilder standard = AmazonS3ClientBuilder.standard();

    if( properties != null )
      {
      String endpoint = properties.getProperty( S3TapProps.S3_ENDPOINT );
      String region = properties.getProperty( S3TapProps.S3_REGION, "us-east-1" );

      if( properties.containsKey( S3TapProps.S3_PROXY_HOST ) )
        {
        ClientConfiguration config = new ClientConfiguration()
          .withProxyHost( properties.getProperty( S3TapProps.S3_PROXY_HOST ) )
          .withProxyPort( PropertyUtil.getIntProperty( properties, S3TapProps.S3_PROXY_PORT, -1 ) );

        standard.withClientConfiguration( config );
        }

      if( endpoint != null )
        standard.withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration( endpoint, region ) );
      else
        standard.setRegion( region );

      if( Boolean.parseBoolean( properties.getProperty( S3TapProps.S3_PATH_STYLE_ACCESS, "false" ) ) )
        standard.enablePathStyleAccess();
      }

    return standard.build();
    }

  /**
   * Method getCheckpointer returns the checkpointer of this S3Tap object.
   *
   * @return the checkpointer (type S3Checkpointer) of this S3Tap object.
   */
  public S3Checkpointer getCheckpointer()
    {
    return checkpointer;
    }

  /**
   * Method getBucketName returns the bucketName of this S3Tap object.
   *
   * @return the bucketName (type String) of this S3Tap object.
   */
  public String getBucketName()
    {
    return bucketName;
    }

  /**
   * Method getKey returns the key of this S3Tap object.
   *
   * @return the key (type String) of this S3Tap object.
   */
  public String getKey()
    {
    return key;
    }

  protected String getMarker()
    {
    if( checkpointer != null )
      return checkpointer.getLastKey( getBucketName() );

    return null;
    }

  protected void setLastMarker( String marker )
    {
    if( checkpointer != null )
      checkpointer.setLastKey( getBucketName(), marker );
    }

  protected void commitMarker()
    {
    if( checkpointer != null )
      checkpointer.commit();
    }

  /**
   * Method getFilter returns the filter of this S3Tap object.
   *
   * @return the filter (type Predicate) of this S3Tap object.
   */
  public Predicate<String> getFilter()
    {
    return filter;
    }

  /**
   * Method getDelimiter returns the delimiter of this S3Tap object.
   *
   * @return the delimiter (type String) of this S3Tap object.
   */
  public String getDelimiter()
    {
    return delimiter;
    }

  @Override
  public String getIdentifier()
    {
    return makeStringIdentifier( getBucketName(), getKey() );
    }

  @Override
  public String getFullIdentifier( Properties conf )
    {
    return getIdentifier();
    }

  @Override
  public boolean deleteResource( Properties conf ) throws IOException
    {
    AmazonS3 s3Client = getS3Client( conf );

    s3Client.deleteObject( getBucketName(), getKey() );

    return true;
    }

  @Override
  public boolean createResource( Properties conf ) throws IOException
    {
    AmazonS3 s3Client = getS3Client( conf );

    s3Client.putObject( getBucketName(), getKey(), "" );

    return true;
    }

  protected ObjectMetadata getObjectMetadata( Properties conf )
    {
    if( objectMetadata == null )
      objectMetadata = getS3Client( conf ).getObjectMetadata( getBucketName(), getKey() );

    return objectMetadata;
    }

  private class CheckedFilterInputStream extends FilterInputStream
    {
    public CheckedFilterInputStream( InputStream inputStream )
      {
      super( inputStream );
      }
    }

  @Override
  public TupleEntryIterator openForRead( FlowProcess<? extends Properties> flowProcess, InputStream input ) throws IOException
    {
    AmazonS3 s3Client = getS3Client( flowProcess.getConfig() );

    final String[] identifier = new String[ 1 ];

    CloseableIterator<InputStream> iterator = new CloseableIterator<InputStream>()
      {
      S3Iterable iterable = S3Iterable.iterable( s3Client, getBucketName(), getKey() )
        .withFilter( getFilter() )
        .withMarker( getMarker() );

      Iterator<S3ObjectSummary> iterator = iterable.iterator();
      InputStream lastInputStream;

      @Override
      public boolean hasNext()
        {
        return iterator.hasNext();
        }

      @Override
      public InputStream next()
        {
        safeClose();

        S3ObjectSummary objectSummary = iterator.next();

        identifier[ 0 ] = makeStringIdentifier( objectSummary.getBucketName(), objectSummary.getKey() );

        flowProcess.getFlowProcessContext().setSourcePath( identifier[ 0 ] );

        if( LOG.isDebugEnabled() )
          LOG.debug( "s3 retrieving: {}/{}, with size: {}", objectSummary.getBucketName(), objectSummary.getKey(), objectSummary.getSize() );

        // getObject does not seem to fill the InputStream, nor does the InputStream support marking
        // may make sense to wrap this iterator in a iterate ahead iterator that attempts to pre-fetch objects in a different thread
        lastInputStream = new CheckedFilterInputStream( s3Client.getObject( objectSummary.getBucketName(), objectSummary.getKey() ).getObjectContent() )
          {
          @Override
          public void close() throws IOException
            {
            setLastMarker( objectSummary.getKey() );
            super.close();
            }
          };

        return lastInputStream;
        }

      private void safeClose()
        {
        try
          {
          if( lastInputStream != null )
            lastInputStream.close();

          lastInputStream = null;
          }
        catch( IOException exception )
          {
          // do nothing
          }
        }

      @Override
      public void close()
        {
        safeClose();
        commitMarker();
        }
      };

    return new TupleEntrySchemeIterator<Properties, InputStream>( flowProcess, this, getScheme(), iterator, () -> identifier[ 0 ] );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Properties> flowProcess, OutputStream outputStream ) throws IOException
    {
    AmazonS3 s3Client = getS3Client( flowProcess.getConfig() );

    if( !s3Client.doesBucketExist( getBucketName() ) )
      s3Client.createBucket( getBucketName() );

    PipedInputStream pipedInputStream = new PipedInputStream();
    PipedOutputStream pipedOutputStream = new PipedOutputStream( pipedInputStream );

    TransferManager transferManager = TransferManagerBuilder.standard().withS3Client( s3Client ).build();

    ObjectMetadata metadata = new ObjectMetadata();

    if( LOG.isDebugEnabled() )
      LOG.debug( "starting upload: {}", getIdentifier() );

    final String key = resolveKey( flowProcess, getKey() );

    Upload upload = transferManager.upload( getBucketName(), key, pipedInputStream, metadata );

    return new TupleEntrySchemeCollector<Properties, OutputStream>( flowProcess, this, getScheme(), pipedOutputStream, makeStringIdentifier( getBucketName(), key ) )
      {
      @Override
      public void close()
        {
        super.close();

        try
          {
          UploadResult uploadResult = upload.waitForUploadResult();

          if( uploadResult != null )
            {
            if( LOG.isDebugEnabled() )
              LOG.debug( "completed upload: {}, with key: {}", getIdentifier(), uploadResult.getKey() );
            }
          }
        catch( InterruptedException exception )
          {
          // ignore
          }
        finally
          {
          transferManager.shutdownNow( false );
          }
        }
      };
    }

  protected String resolveKey( FlowProcess<? extends Properties> flowProcess, String key )
    {
    int partNum = flowProcess.getIntegerProperty( PartitionTap.PART_NUM_PROPERTY, 0 );

    key = key.replace( SEQUENCE_TOKEN, String.format( "%05d", partNum ) );

    if( getScheme() instanceof FileFormat )
      return key + "." + ( (FileFormat) getScheme() ).getExtension();

    return key;
    }

  @Override
  public boolean resourceExists( Properties conf ) throws IOException
    {
    if( getKey() == null )
      return getS3Client( conf ).doesBucketExist( getBucketName() );

    return getKey().endsWith( "/" ) || getS3Client( conf ).doesObjectExist( getBucketName(), getKey() );
    }

  @Override
  public long getModifiedTime( Properties conf ) throws IOException
    {
    return getObjectMetadata( conf ).getLastModified().getTime();
    }

  @Override
  public boolean isDirectory( FlowProcess<? extends Properties> flowProcess ) throws IOException
    {
    return MIME_DIRECTORY.equalsIgnoreCase( getObjectMetadata( flowProcess.getConfig() ).getContentType() );
    }

  @Override
  public boolean isDirectory( Properties conf ) throws IOException
    {
    return isDirectory( FlowProcess.nullFlowProcess() );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Properties> flowProcess ) throws IOException
    {
    return getChildIdentifiers( flowProcess.getConfig() );
    }

  @Override
  public String[] getChildIdentifiers( Properties conf ) throws IOException
    {
    return getChildIdentifiers( conf, 1, false );
    }

  @Override
  public String[] getChildIdentifiers( FlowProcess<? extends Properties> flowProcess, int depth, boolean fullyQualified ) throws IOException
    {
    return getChildIdentifiers( flowProcess.getConfig(), depth, fullyQualified );
    }

  @Override
  public String[] getChildIdentifiers( Properties conf, int depth, boolean fullyQualified ) throws IOException
    {
    if( !resourceExists( conf ) )
      return new String[ 0 ];

    S3Iterable objects = S3Iterable.iterable( getS3Client( conf ), getBucketName(), getKey() )
      .withDelimiter( getDelimiter() )
      .withMaxDepth( depth )
      .withFilter( getFilter() )
      .withMarker( getMarker() );

    Iterator<S3ObjectSummary> iterator = objects.iterator();

    List<String> results = new ArrayList<>();

    while( iterator.hasNext() )
      results.add( makePath( iterator, fullyQualified ) );

    return results.toArray( new String[ results.size() ] );
    }

  protected String makePath( Iterator<S3ObjectSummary> iterator, boolean fullyQualified )
    {
    String key = iterator.next().getKey();

    if( fullyQualified )
      return makeStringIdentifier( getBucketName(), key );

    return key.substring( getKey().length() );
    }

  @Override
  public long getSize( FlowProcess<? extends Properties> flowProcess ) throws IOException
    {
    return getSize( flowProcess.getConfig() );
    }

  @Override
  public long getSize( Properties conf ) throws IOException
    {
    if( isDirectory( conf ) )
      return 0;

    return getObjectMetadata( conf ).getInstanceLength();
    }

  protected static String makeStringIdentifier( String bucketName, String keyPrefix )
    {
    if( isEmpty( keyPrefix ) )
      return String.format( "s3://%s/", bucketName );

    return String.format( "s3://%s/%s", bucketName, keyPrefix );
    }
  }
