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

package cascading.aws.s3.logs;

import java.util.TimeZone;

import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.DateType;

/**
 */
public class S3Logs
  {
  public static final String REGEX = "(\\S+) ([a-z0-9][a-z0-9-.]+) \\[(.*\\+.*)] (\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b) (\\S+) (\\S+) (\\S+) (\\S+) \"(\\w+ \\S+ \\S+)\" (\\d+|-) (\\S+) (\\d+|-) (\\d+|-) (\\d+|-) (\\d+|-) \"(https?://.*/?|-)\" \"(.*)\" (\\S+)";

  public static final CleanCoercibleType<Long> CLEAN_LONG = new CleanCoercibleType<>( Coercions.LONG_OBJECT, o -> o.equals( "-" ) ? null : o );
  public static final CleanCoercibleType<String> CLEAN_STRING = new CleanCoercibleType<>( Coercions.STRING, o -> o.equals( "-" ) ? null : o );

  public static Fields BUCKET_OWNER = new Fields( "bucketOwner", String.class );
  public static Fields BUCKET = new Fields( "bucket", String.class );
  public static Fields TIME = new Fields( "time", new DateType( "dd/MMM/yyyy:HH:mm:ss Z", TimeZone.getTimeZone( "UTC" ) ) );
  public static Fields REMOTE_IP_ADDRESS = new Fields( "remoteIpAddress", String.class );
  public static Fields REQUESTER = new Fields( "requester", CLEAN_STRING );
  public static Fields REQUEST_ID = new Fields( "requestId", String.class );
  public static Fields OPERATION = new Fields( "operation", String.class );
  public static Fields KEY = new Fields( "key", CLEAN_STRING );
  public static Fields REQUEST_URI = new Fields( "requestUri", String.class );
  public static Fields HTTP_STATUS = new Fields( "httpStatus", Integer.class );
  public static Fields ERROR_CODE = new Fields( "errorCode", CLEAN_STRING );
  public static Fields BYTES_SENT = new Fields( "bytesSent", CLEAN_LONG );
  public static Fields OBJECT_SIZE = new Fields( "objectSize", CLEAN_LONG );
  public static Fields TOTAL_TIME = new Fields( "totalTime", CLEAN_LONG );
  public static Fields TURN_AROUND_TIME = new Fields( "turnAroundTime", CLEAN_LONG );
  public static Fields REFERRER = new Fields( "referrer", CLEAN_STRING );
  public static Fields USER_AGENT = new Fields( "userAgent", String.class );
  public static Fields VERSION_ID = new Fields( "versionId", CLEAN_STRING );

  public static final Fields FIELDS = Fields.NONE
    .append( BUCKET_OWNER )
    .append( BUCKET )
    .append( TIME )
    .append( REMOTE_IP_ADDRESS )
    .append( REQUESTER )
    .append( REQUEST_ID )
    .append( OPERATION )
    .append( KEY )
    .append( REQUEST_URI )
    .append( HTTP_STATUS )
    .append( ERROR_CODE )
    .append( BYTES_SENT )
    .append( OBJECT_SIZE )
    .append( TOTAL_TIME )
    .append( TURN_AROUND_TIME )
    .append( REFERRER )
    .append( USER_AGENT )
    .append( VERSION_ID );
  }
