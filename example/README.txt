ParStream Example for the Json adaptor for ParStream
====================================================

This presents an example on how to use the Json adaptor for ParStream.
The Json adaptor converts Json objects to ParStream rows.

First, instructions to start the ParStream server instance with the database used by
this example.

Follows that, two possible ways to build and execute the example. The first option
uses maven, and the second one uses console. You are free to choose the option that
suits you best.

--------------------------------------------------------
Start server with database defined in the "conf" folder:
--------------------------------------------------------

  1- From console, execute:
     - $PARSTREAM_HOME/bin/parstream-server json

  NOTE:
    - Ensure you use the correct path to "parstream-server"

  2- Export java.library.path by executing:
     - export LD_LIBRARY_PATH=$PARSTREAM_HOME/lib

------------
Assumptions:
------------
  1- $MVN_HOME=~/.m2/repository 

-------------------------------------------
1) Build and execute example using maven --
-------------------------------------------

The maven project has a dependency on the "ps-streaming-import-<PARSTREAM_VERSION>.jar" found in $PARSTREAM_HOME/lib.
You first need to install the "ps-streaming-import-<PARSTREAM_VERSION>.jar" into you local maven repository.

  1- Navigate to:
     - $PARSTREAM_HOME/lib

  2- Execute:
     - mvn install:install-file -Dfile=ps-streaming-import-<PARSTREAM_VERSION>.jar -DgroupId=com.parstream.driver -DartifactId=ps-streaming-import -Dversion=<PARSTREAM_VERSION> -Dpackaging=jar

  3- Navigate to:
     - $PARSTREAM_HOME/adaptor/lib

  4- Execute:
     - mvn install:install-file -Dfile=json-decoder-<ADAPTOR_VERSION>.jar -DgroupId=com.parstream.adaptor -DartifactId=json-decoder -Dversion=<ADAPTOR_VERSION> -Dpackaging=jar


Then, you need to compile and run the example.

  5- Navigate to:
     - $PARSTREAM_HOME/adaptor/example/json/example

  6- Execute:
     - mvn install

  7- Navigate to:
     - $MVN_HOME/com/parstream/adaptor/json-example/<ADAPTOR_VERSION>

  8- Execute:
     - java -jar json-example-<ADAPTOR_VERSION>.jar localhost 8999

     NOTE:
       - The json example executable JAR expects two command line arguments:
         1- "localhost" the host address on which the ParStream server is running
         2- "8999"      the port number on which the ParStream server is running


---------------------------------------------
2) Build and execute example using console --
---------------------------------------------

  1- Download the javax.json-<VERSION>.jar from:
     - https://jsonp.java.net/download.html
     - Now on, the assumption is made that: $JSON_PATH points to your downloaded JAR file

  2- Navigate to:
     -  $PARSTREAM_HOME/adaptor/example/json/example/src/main/java/

  3- Compile "JsonAdaptorExample":
     - javac -cp $PARSTREAM_HOME/adaptor/lib/json-decoder-<ADAPTOR_VERSION>.jar:$PARSTREAM_HOME/lib/ps-streaming-import-<PARSTREAM_VERSION>.jar:$JSON_PATH JsonAdaptorExample.java

  4- Execute "JsonAdaptorExample":
     - java -cp $PARSTREAM_HOME/adaptor/lib/json-decoder-<ADAPTOR_VERSION>.jar:$PARSTREAM_HOME/lib/ps-streaming-import-<PARSTREAM_VERSION>.jar:$JSON_PATH:. JsonAdaptorExample localhost 8999 ../resources/json.ini ../resources/data.json

     NOTE:
       - The JsonAdaptorExample expects four command line arguments:
         1- "localhost" the host address on which the ParStream server is running
         2- "8999"      the port number on which the ParStream server is running
         3- path to the column mapping configuration file
         4- path to the file containing json records


-------------------------------------
-- Notes for executing from an IDE --
-------------------------------------

In case you would like to execute the example from your IDE, ensure:
  * the path "-Djava.library.path" is set
  * the command line arguments are correctly set
