/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class FSDigestInputStream is an {@link FSInputStream} implementation that can verify a
 * {@link MessageDigest} and will count the number of bytes read for use in progress status.
 */
public class FSDigestInputStream extends FSInputStream
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( FSDigestInputStream.class );

  /** Field count */
  int count = 0;
  /** Field inputStream */
  final InputStream inputStream;
  /** Field digestHex */
  final String digestHex;

  /**
   * Constructor FSDigestInputStream creates a new FSDigestInputStream instance.
   *
   * @param inputStream of type InputStream
   * @param digestHex   of type String
   * @throws IOException if unable to get md5 digest
   */
  public FSDigestInputStream( InputStream inputStream, String digestHex ) throws IOException
    {
    this( inputStream, getMD5Digest(), digestHex );
    }

  /**
   * Constructor FSDigestInputStream creates a new FSDigestInputStream instance.
   *
   * @param inputStream   of type InputStream
   * @param messageDigest of type MessageDigest
   * @param digestHex     of type String
   */
  public FSDigestInputStream( InputStream inputStream, MessageDigest messageDigest, String digestHex )
    {
    this.inputStream = digestHex == null ? inputStream : new DigestInputStream( inputStream, messageDigest );
    this.digestHex = digestHex;
    }

  /**
   * Method getMD5Digest returns the MD5Digest of this FSDigestInputStream object.
   *
   * @return the MD5Digest (type MessageDigest) of this FSDigestInputStream object.
   * @throws IOException when
   */
  private static MessageDigest getMD5Digest() throws IOException
    {
    try
      {
      return MessageDigest.getInstance( "MD5" );
      }
    catch( NoSuchAlgorithmException exception )
      {
      throw new IOException( "digest not found: " + exception.getMessage() );
      }
    }

  @Override
  public int read() throws IOException
    {
    count++;
    return inputStream.read();
    }

  @Override
  public int read( byte[] b, int off, int len ) throws IOException
    {
    int result = inputStream.read( b, off, len );
    count += result;
    return result;
    }

  @Override
  public void close() throws IOException
    {
    inputStream.close();

    LOG.info( "closing stream, testing digest: [{}]", digestHex == null ? "none" : digestHex );

    if( digestHex == null )
      return;

    String digestHex = new String( Hex.encodeHex( ( (DigestInputStream) inputStream ).getMessageDigest().digest() ) );

    if( !digestHex.equals( this.digestHex ) )
      {
      String message = "given digest: [" + this.digestHex + "], does not match input stream digest: [" + digestHex + "]";
      LOG.error( message );
      throw new IOException( message );
      }
    }

  @Override
  public void seek( long pos ) throws IOException
    {
    if( getPos() == pos )
      return;

    if( getPos() > pos )
      throw new IOException( "cannot seek to " + pos + ", currently at" + getPos() );

    int len = (int) ( pos - getPos() );
    byte[] bytes = new byte[ 50 * 1024 ];

    while( len > 0 )
      len -= read( bytes, 0, Math.min( len, bytes.length ) );
    }

  @Override
  public long getPos() throws IOException
    {
    return count;
    }

  @Override
  public boolean seekToNewSource( long targetPos ) throws IOException
    {
    return false;
    }
  }
