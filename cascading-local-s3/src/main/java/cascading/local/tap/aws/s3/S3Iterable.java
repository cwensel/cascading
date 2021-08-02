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

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Predicate;

import cascading.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class S3Iterable implements Iterable<S3ObjectSummary>
  {
  private static final Logger LOG = LoggerFactory.getLogger( S3Iterable.class );

  private AmazonS3 client;
  private String bucketName;
  private String prefix;
  private String marker;
  private String delimiter;
  private Predicate<String> filter;
  private int maxDepth = Integer.MAX_VALUE;
  private Integer batchSize;

  class S3ObjectIterator implements Iterator<S3ObjectSummary>
    {
    private int depth = 0;
    private Queue<String> prefixes = new LinkedList<>();
    private ObjectListing current = null;
    private Iterator<S3ObjectSummary> iterator = null;

    protected S3ObjectIterator()
      {
      if( !Util.isEmpty( getPrefix() ) )
        prefixes.offer( getPrefix() );
      }

    @Override
    public boolean hasNext()
      {
      return currentIterator().hasNext();
      }

    @Override
    public S3ObjectSummary next()
      {
      return currentIterator().next();
      }

    @Override
    public void remove()
      {
      throw new UnsupportedOperationException();
      }

    private Iterator<S3ObjectSummary> currentIterator()
      {
      while( depth < maxDepth && ( current == null || ( !iterator.hasNext() && ( current.isTruncated() || !prefixes.isEmpty() ) ) ) )
        {
        if( current != null && !iterator.hasNext() && !current.isTruncated() && !prefixes.isEmpty() )
          current = null;

        long startTime = System.currentTimeMillis();

        if( current == null )
          current = getClient().listObjects( createRequest() );
        else
          current = getClient().listNextBatchOfObjects( current );

        long duration = System.currentTimeMillis() - startTime;

        prefixes.addAll( current.getCommonPrefixes() );

        List<S3ObjectSummary> objectSummaries = current.getObjectSummaries();

        if( LOG.isDebugEnabled() )
          LOG.debug( "s3 {}/{} listed items: {}, with max: {}, in: {}", bucketName, prefix == null ? "" : prefix, objectSummaries.size(), current.getMaxKeys(), Duration.ofMillis( duration ) );

        if( filter != null )
          iterator = objectSummaries.stream().filter( s -> filter.test( s.getKey() ) ).iterator();
        else
          iterator = objectSummaries.iterator();
        }

      return iterator;
      }

    protected ListObjectsRequest createRequest()
      {
      depth++;

      return new ListObjectsRequest()
        .withBucketName( getBucketName() )
        .withPrefix( prefixes.poll() )
        .withMaxKeys( getBatchSize() )
        .withDelimiter( getDelimiter() )
        .withMarker( getMarker() );
      }
    }

  public static S3Iterable iterable( AmazonS3 s3, String bucketName, String prefix )
    {
    S3Iterable iterable = new S3Iterable( s3, bucketName );

    iterable.prefix = prefix;

    return iterable;
    }

  private S3Iterable( AmazonS3 client, String bucketName )
    {
    this.client = client;
    this.bucketName = bucketName;
    }

  public S3Iterable withMarker( String marker )
    {
    this.marker = marker;
    return this;
    }

  public S3Iterable withBatchSize( int batchSize )
    {
    this.batchSize = batchSize;
    return this;
    }

  public S3Iterable withDelimiter( String delimiter )
    {
    this.delimiter = delimiter;
    return this;
    }

  public S3Iterable withFilter( Predicate<String> filter )
    {
    this.filter = filter;
    return this;
    }

  public S3Iterable withMaxDepth( int depth )
    {
    this.maxDepth = depth;
    return this;
    }

  public AmazonS3 getClient()
    {
    return client;
    }

  public Integer getBatchSize()
    {
    return batchSize;
    }

  public String getBucketName()
    {
    return bucketName;
    }

  public String getPrefix()
    {
    return prefix;
    }

  public String getMarker()
    {
    return marker;
    }

  public Predicate<String> getFilter()
    {
    return filter;
    }

  public String getDelimiter()
    {
    return delimiter;
    }

  public int getMaxDepth()
    {
    return maxDepth;
    }

  @Override
  public Iterator<S3ObjectSummary> iterator()
    {
    return new S3ObjectIterator();
    }
  }
