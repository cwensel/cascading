/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.log4j.Logger;

/**
 * Class FSDigestInputStream is an {@link FSInputStream} implementation that can verify a
 * {@link MessageDigest} and will count the number of bytes read for use in progress status.
 */
public class FSDigestInputStream extends FSInputStream
  {
  /** Field LOG */
  private static final Logger LOG = Logger.getLogger( FSDigestInputStream.class );

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

    LOG.info( "closing stream, testing digest: [" + ( digestHex == null ? "none" : digestHex ) + "]" );

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
