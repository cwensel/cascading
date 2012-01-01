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

package cascading.flow.hadoop;

/**
 *
 */
public class HadoopProperties
  {
  public static final String COGROUP_SPILL_THRESHOLD = "cascading.cogroup.spill.threshold";
  public static final String COGROUP_SPILL_COMPRESS = "cascading.cogroup.spill.compress";
  public static final String COGROUP_SPILL_CODECS = "cascading.cogroup.spill.codecs";

  public static final int defaultThreshold = 10 * 1000;
  public static final String defaultCodecs = "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec";

  public static final String JOIN_SPILL_THRESHOLD = "cascading.join.spill.threshold";
  public static final String JOIN_SPILL_COMPRESS = "cascading.join.spill.compress";
  public static final String JOIN_SPILL_CODECS = "cascading.join.spill.codecs";
  }
