CommonLog2SQLite
================

Converts an apache common log to a sqlite database.

Requirements
------------
* perl (5.014)

Perl Modules
------------
------------
* List::MoreUtils;
* DBI;
* Data::Dumper;
* DateTime;
* Getopt::Long;

How to run
----------



The easiest way is just to:
> perl commonLogToSQLite.pl <log_file>

This will create a common_log.db file. This file is a sqlite file containing all the requests
from the <log_file> parsed and inserted. 


The more complexed way:
> perl commonLogToSQLite.pl --batch 100000 --db my_sqlite.db <log_file>

This does the same as the previous example, but it will create a database named my_sqlite.db and
it will perform the insertions on the database using batches of 100000 requests.

Note
----
----
If the database doesn't exists it will created, otherwise
the requests from the log will be inserted in the database.



Internals
---------

The database will contain only one table. The table name is REQUESTS:

| Column            | Type    | Constraints | Description                                                                     |
|-------------------+---------+-------------+---------------------------------------------------------------------------------|
| ORIGIN_IP         | TEXT    | NOT NULL    | The IP of the requester                                                         |
| USER_CLIENT       | TEXT    |             | The user identifier                                                             |
| USER              | TEXT    |             | The user id                                                                     |
| TIMESTAMP         | INTEGER | NOT NULL    | The timestamp in unix epoch                                                     |
| REQUEST_METHOD    | TEXT    | NOT NULL    | The request method (GET,POST,PUT...)                                            |
| RESOURCE          | TEXT    | NOT NULL    | The resource of the request                                                     |
| PROTOCOL          | TEXT    |             | The protocol (basically it's always HTTP)                                       |
| PROTOCOL_VERSION  | REAL    |             | The version of the protocol (for HTTP protocol, it should be 1.0 or 1.1)        |
| STATUS            | INTEGER | NOT NULL    | The status code of the response (for HTTP protocol it should be 200,404,401...) |
| BYTES_TRANSFERRED | INTEGER |             | The size (in bytes) of the response                                             |
|-------------------+---------+-------------+---------------------------------------------------------------------------------|


Bugs && questions
-----------------

Please e-mail me at demanuel<at>ymail.com
