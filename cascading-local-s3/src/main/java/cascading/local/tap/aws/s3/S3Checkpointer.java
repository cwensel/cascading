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

/**
 * Interface S3Checkpointer is used to retrieve and persist last known keys for any given S3 bucket.
 *
 * @see S3FileCheckpointer
 */
public interface S3Checkpointer
  {
  /**
   * Method getLastKey returns the last known key or null for the requested bucket.
   *
   * @param bucketName of String
   * @return String
   */
  String getLastKey( String bucketName );

  /**
   * Method setLastKey stores the last seen and consumed key for the given bucket.
   *
   * @param bucketName of String
   * @param key        of String
   */
  void setLastKey( String bucketName, String key );

  /**
   * Method commit notifies the instance that no more keys will be handled.
   */
  void commit();
  }
