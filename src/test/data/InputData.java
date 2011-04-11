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

package data;

public interface InputData
  {
  String inputPath = "src/test/data/";

  String inputFileApache = inputPath + "apache.10.txt";
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

  String inputPageData = inputPath + "url+page.200.txt";

  String testDelimited = inputPath + "delimited.txt";
  String testDelimitedSpecialCharData = inputPath + "delimited-spec-char.txt";

  String inputFileComments = inputPath + "comments+lower.txt";
  }
