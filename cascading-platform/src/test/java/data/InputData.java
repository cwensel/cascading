/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package data;/*
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

public interface InputData
  {
  public static final String TEST_DATA_PATH = "test.data.path";

  String inputPath = System.getProperty( TEST_DATA_PATH, "../cascading-platform/src/test/resources/data/" );

  String inputFileApache = inputPath + "apache.10.txt";
  String inputFileApacheClean = inputPath + "apache-clean.10.txt";
  String inputFileApache200 = inputPath + "apache.200.txt";
  String inputFileIps = inputPath + "ips.20.txt";
  String inputFileNums20 = inputPath + "nums.20.txt";
  String inputFileNums10 = inputPath + "nums.10.txt";
  String inputFileCritics = inputPath + "critics.txt";
  String inputFileUpper = inputPath + "upper.txt";
  String inputFileLower = inputPath + "lower.txt";
  String inputFileLowerOffset = inputPath + "lower-offset.txt";
  String inputFileJoined = inputPath + "lower+upper.txt";
  String inputFileJoinedExtra = inputPath + "extra+lower+upper.txt";
  String inputFileLhs = inputPath + "lhs.txt";
  String inputFileRhs = inputPath + "rhs.txt";
  String inputFileCross = inputPath + "lhs+rhs-cross.txt";
  String inputFileLhsSparse = inputPath + "lhs-sparse.txt";
  String inputFileRhsSparse = inputPath + "rhs-sparse.txt";

  String testDelimited = inputPath + "delimited.txt";
  String testDelimitedHeader = inputPath + "delimited-header.txt";
  String testDelimitedSpecialCharData = inputPath + "delimited-spec-char.txt";
  String testDelimitedExtraField = inputPath + "delimited-extra-field.txt";

  String inputFileComments = inputPath + "comments+lower.txt";

  String testClasspathJar = inputPath + "test-classpath.jar";
  String testClasspathJarContents = "apache.10.txt";
  }
