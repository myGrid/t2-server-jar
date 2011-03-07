Taverna Server Java Client Library
----------------------------------

This library provides a Java API to access a Taverna Server instance through
its REST API.

Building
--------

Simply use maven from the root directory:
$ mvn install

To build documentation:
$ mvn javadoc:javadoc
or add 'javadoc:javadoc' to the 'install' line above.

Usage
-----

The easiest way of using this library is to include it in your top-level
pom.xml file so all its dependencies can be automatically added to your
project as well.

It is available from the following maven repository:
http://www.mygrid.org.uk/maven/repository

Disclaimer
----------

This API is to be considered in flux until version 1.0. Until then methods
may be deprecated at short notice, although this will be kept to an absolute
minimum as far as possible.
