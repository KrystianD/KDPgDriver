KDPgDriver
======

KDPgDriver - LINQ-based, type safe query builder, query executor and results parser into plain C# objects.
Uses Npgsql as PostgreSQL connector.

**Key Concepts**

* Supports PostgreSQL only,
* Single source of truth - table and column names defined as attributes in POCO model.

**Features**

* Support for SELECT, INSERT, DELETE, UPDATE queries,
* Support for typed LEFT JOINs,
* Support for json(b) fields (querying, extracting data from nested models),
* Subqueries support (for INSERT statement as of now),
* Support for transactions and batch queries.

**Examples**

[Basic examples](examples.md)