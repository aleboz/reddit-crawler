reddit-crawler
==============

A distributed reddit crawler written in Java using RMI (Remote Method Invocation), that dumps post and comment information into SQLite3 databases. Based off of the reddit API (https://github.com/reddit/reddit/wiki/API).

Licenses and attributions
-------------------------

All source code ca.uwaterloo.sharvey.* is licensed under the MIT License (see LICENSE).

All source code org.json.* is licensed under the JSON license (http://www.json.org/license.html)

The jar file at lib/sqlite-jdbc-x.x.x.jar is licensed under the Apache License version 2.0 (http://www.xerial.org/trac/Xerial/wiki/SQLiteJDBC#License)

TODO
----

 * Don't hardcode some things, make them flags instead
 * Generate a jarfile
 * Generate a build.xml (for use with ant) OR Makefile
 * Write more documentation
