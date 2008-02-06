
Thanks for using Cascading.

General Information:

  Project and contact information: http://www.cascading.org/

  This distribution includes three Cascading jar files:

  cascading.jar  - Cascading class files and libraries in the 'lib' folder
  cascading-lib.jar - all Cascading class files
  cascading-examples.jar - compiled classes from 'src/examples'

  To use with Hadoop, you only need the cascading.jar. We suggest merging your class files and libraries with this jar
  and executing via 'hadoop jar your.jar <your args>'. Hadoop will unpack the jar and add any libraries in 'lib' to the
  classpath.

  This ant task works quite well:

    <target name="jar" depends="build">

      <unjar dest="${build.classes}" src="${cascading.lib}"/>

      <jar jarfile="${build.dir}/${ant.project.name}.jar">
        <fileset dir="${build.classes}"/>
        <fileset dir="${basedir}" includes="lib/"/>
        <manifest>
          <attribute name="Main-Class" value="${ant.project.name}/Main"/>
        </manifest>
      </jar>

    </target>

  Where the 'cascading.lib' property points to the cascading.jar. Remember cascading.jar already has a 'lib' folder
  embedded in it. Alternatively you could just add the cascading libs and cascading-lib.jar to your 'lib' folder.

License:

  Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.

  Cascading is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  Cascading is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Cascading.  If not, see <http://www.gnu.org/licenses/>.

Knowns Issues:

  There is one current limitation that will be resolved:

   * With an Every -> Every chain, the second Every cannot reference a field created by the previous.