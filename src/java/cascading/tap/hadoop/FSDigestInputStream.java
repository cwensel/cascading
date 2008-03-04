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

package cascading.tap.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.FSInputStream;

/**
 *
 */
public class FSDigestInputStream extends FSInputStream
  {
  int count = 0;
  InputStream inputStream;
  String digestHex;

  public FSDigestInputStream( InputStream inputStream, String digestHex ) throws IOException
    {
    this( inputStream, getMD5Digest(), digestHex );
    }

  public FSDigestInputStream( InputStream inputStream, MessageDigest messageDigest, String digestHex )
    {
    this.inputStream = digestHex == null ? inputStream : new DigestInputStream( inputStream, messageDigest );
    this.digestHex = digestHex;
    }

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

  public int read() throws IOException
    {
    count++;
    return inputStream.read();
    }

  public int read( byte[] b, int off, int len ) throws IOException
    {
    int result = inputStream.read( b, off, len );
    count += result;
    return result;
    }

  public void close() throws IOException
    {
    inputStream.close();

    if( digestHex == null )
      return;

    String digestHex = new String( Hex.encodeHex( ( (DigestInputStream) inputStream ).getMessageDigest().digest() ) );

    if( !digestHex.equals( this.digestHex ) )
      throw new IOException( "given digest: [" + this.digestHex + "], does not match input stream digest: [" + digestHex + "]" );
    }

  public void seek( long pos ) throws IOException
    {
    throw new IOException( "not supported" );
    }

  public long getPos() throws IOException
    {
    return count;
    }

  public boolean seekToNewSource( long targetPos ) throws IOException
    {
    return false;
    }
  }
