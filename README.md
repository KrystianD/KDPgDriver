KDPgDriver
======

KDPgDriver - LINQ-based, type safe query builder, query executor and results parser into plain C# objects.
Uses Npgsql as PostgreSQL connector.

**Key Concepts**

* Made specifically for PostgreSQL,
* Single source of truth - table and column names defined as attributes in POCO model,
* Stable - almost 200 tests (unit and functional).

**Features**

* Support for SELECT (including EXISTS), INSERT, DELETE and UPDATE queries,
* Support for typed LEFT JOINs,
* Support for json(b) fields (querying, extracting data from nested models),
* Implemented some of Postgres functions in type-safe manner,
* Support for using C# methods of string (StartsWith, Contains, etc) and DateTime properties (Day, Month, etc),
* Extension methods for easy LIKE, IN and ANY usage,
* Subqueries support:
  * in WHERE clause of SELECT, DELETE and UPDATE queries,
  * in INSERT query as field value,
* Support for transactions and batch queries (and batch transaction queries),
* Support for placing results in custom DTO objects.

**Examples**

[Basic examples](examples.md)