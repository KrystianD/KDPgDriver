﻿using System;
using System.Collections.Generic;
using System.Linq;

namespace KDPgDriver.Utils
{
  public static class EscapeUtils
  {
    public static string QuoteObjectName(string str)
    {
      if (str.ToHashSet().IsSubsetOf(ValidObjectNameChars) && !ReservedKeywords.Contains(str.ToLower()))
        return str;
      else
        return "\"" + str + "\"";
    }

    public static string QuoteTable(string tableName, string schema = null)
    {
      return schema == null ? QuoteObjectName(tableName) : $"{QuoteObjectName(schema)}.{QuoteObjectName(tableName)}";
    }

    public static string EscapePostgresValue(object value)
    {
      return value switch {
          string s => "'" + s.Replace("'", "''") + "'",
          short v => v.ToString(),
          int v => v.ToString(),
          long v => v.ToString(),
          _ => throw new Exception($"unable to escape value of type: {value.GetType()}"),
      };
    }

    // Constants
    // ReSharper disable StringLiteralTypo
    private static readonly HashSet<string> ReservedKeywords = new HashSet<string>() {
        "a", "abort", "abs", "absent", "absolute", "access", "according", "action", "ada", "add", "admin", "after", "aggregate",
        "all", "allocate", "also", "alter", "always", "analyse", "analyze", "and", "any", "are", "array", "array_agg",
        "array_max_cardinality", "as", "asc", "asensitive", "assertion", "assignment", "asymmetric", "at", "atomic",
        "attribute", "attributes", "authorization", "avg", "backward", "base64", "before", "begin", "begin_frame",
        "begin_partition", "bernoulli", "between", "bigint", "binary", "bit", "bit_length", "blob", "blocked", "bom", "boolean",
        "both", "breadth", "by", "c", "cache", "call", "called", "cardinality", "cascade", "cascaded", "case", "cast", "catalog",
        "catalog_name", "ceil", "ceiling", "chain", "char", "character", "characteristics", "characters", "character_length",
        "character_set_catalog", "character_set_name", "character_set_schema", "char_length", "check", "checkpoint", "class",
        "class_origin", "clob", "close", "cluster", "coalesce", "cobol", "collate", "collation", "collation_catalog",
        "collation_name", "collation_schema", "collect", "column", "columns", "column_name", "command_function",
        "command_function_code", "comment", "comments", "commit", "committed", "concurrently", "condition", "condition_number",
        "configuration", "conflict", "connect", "connection", "connection_name", "constraint", "constraints",
        "constraint_catalog", "constraint_name", "constraint_schema", "constructor", "contains", "content", "continue",
        "control", "conversion", "convert", "copy", "corr", "corresponding", "cost", "count", "covar_pop", "covar_samp",
        "create", "cross", "csv", "cube", "cume_dist", "current", "current_catalog", "current_date",
        "current_default_transform_group", "current_path", "current_role", "current_row", "current_schema", "current_time",
        "current_timestamp", "current_transform_group_for_type", "current_user", "cursor", "cursor_name", "cycle", "data",
        "database", "datalink", "date", "datetime_interval_code", "datetime_interval_precision", "day", "db", "deallocate",
        "dec", "decimal", "declare", "default", "defaults", "deferrable", "deferred", "defined", "definer", "degree", "delete",
        "delimiter", "delimiters", "dense_rank", "depends", "depth", "deref", "derived", "desc", "describe", "descriptor",
        "deterministic", "diagnostics", "dictionary", "disable", "discard", "disconnect", "dispatch", "distinct", "dlnewcopy",
        "dlpreviouscopy", "dlurlcomplete", "dlurlcompleteonly", "dlurlcompletewrite", "dlurlpath", "dlurlpathonly",
        "dlurlpathwrite", "dlurlscheme", "dlurlserver", "dlvalue", "do", "document", "domain", "double", "drop", "dynamic",
        "dynamic_function", "dynamic_function_code", "each", "element", "else", "empty", "enable", "encoding", "encrypted",
        "end", "en", "end_frame", "end_partition", "enforced", "enum", "equals", "escape", "event", "every", "except",
        "exception", "exclude", "excluding", "exclusive", "exec", "execute", "exists", "exp", "explain", "expression",
        "extension", "external", "extract", "false", "family", "fetch", "file", "filter", "final", "first", "first_value",
        "flag", "float", "floor", "following", "for", "force", "foreign", "fortran", "forward", "found", "frame_row", "free",
        "freeze", "from", "fs", "full", "function", "functions", "fusion", "g", "general", "generated", "get", "global", "go",
        "goto", "grant", "granted", "greatest", "group", "grouping", "groups", "handler", "having", "header", "hex", "hierarchy",
        "hold", "hour", "id", "identity", "if", "ignore", "ilike", "immediate", "immediately", "immutable", "implementation",
        "implicit", "import", "in", "including", "increment", "indent", "index", "indexes", "indicator", "inherit", "inherits",
        "initially", "inline", "inner", "inout", "input", "insensitive", "insert", "instance", "instantiable", "instead",
        "int", "integer", "integrity", "intersect", "intersection", "interval", "into", "invoker", "is", "isnull", "isolation",
        "join", "k", "key", "key_member", "key_type", "label", "lag", "language", "large", "last", "last_value", "lateral",
        "lead", "leading", "leakproof", "least", "left", "length", "level", "library", "like", "like_regex", "limit", "link",
        "listen", "ln", "load", "local", "localtime", "localtimestamp", "location", "locator", "lock", "locked", "logged",
        "lower", "m", "map", "mapping", "match", "matched", "materialized", "max", "maxvalue", "max_cardinality", "member",
        "merge", "message_length", "message_octet_length", "message_text", "method", "min", "minute", "minvalue", "mod",
        "mode", "modifies", "module", "month", "more", "move", "multiset", "mumps", "name", "names", "namespace", "national",
        "natural", "nchar", "nclob", "nesting", "new", "next", "nfc", "nfd", "nfkc", "nfkd", "nil", "no", "none", "normalize",
        "normalized", "not", "nothing", "notify", "notnull", "nowait", "nth_value", "ntile", "null", "nullable", "nullif",
        "nulls", "number", "numeric", "object", "occurrences_regex", "octets", "octet_length", "of", "off", "offset", "oids",
        "old", "on", "only", "open", "operator", "option", "options", "or", "order", "ordering", "ordinality", "others", "out",
        "outer", "output", "over", "overlaps", "overlay", "overriding", "owned", "owner", "p", "pad", "parallel", "parameter",
        "parameter_mode", "parameter_name", "parameter_ordinal_position", "parameter_specific_catalog",
        "parameter_specific_name", "parameter_specific_schema", "parser", "partial", "partition", "pascal", "passing",
        "passthrough", "password", "path", "percent", "percentile_cont", "percentile_disc", "percent_rank", "period",
        "permission", "placing", "plans", "pli", "policy", "portion", "position", "position_regex", "power", "precedes",
        "preceding", "precision", "prepare", "prepared", "preserve", "primary", "prior", "privileges", "procedural", "procedure",
        "program", "public", "quote", "range", "rank", "read", "reads", "real", "reassign", "recheck", "recovery", "recursive",
        "ref", "references", "referencing", "refresh", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2",
        "regr_slope", "regr_sxx", "regr_sxy", "regr_syy", "reindex", "relative", "release", "rename", "repeatable", "replace",
        "replica", "requiring", "reset", "respect", "restart", "restore", "restrict", "result", "return", "returned_cardinality",
        "returned_length", "returned_octet_length", "returned_sqlstate", "returning", "returns", "revoke", "right", "role",
        "rollback", "rollup", "routine", "routine_catalog", "routine_name", "routine_schema", "row", "rows", "row_count",
        "row_number", "rule", "savepoint", "scale", "schema", "schema_name", "scope", "scope_catalog", "scope_name",
        "scope_schema", "scroll", "search", "second", "section", "security", "select", "selective", "self", "sensitive",
        "sequence", "sequences", "serializable", "server", "server_name", "session", "session_user", "set", "setof", "sets",
        "share", "show", "similar", "simple", "size", "skip", "smallint", "snapshot", "some", "source", "space", "specific",
        "specifictype", "specific_name", "sql", "sqlcode", "sqlerror", "sqlexception", "sqlstate", "sqlwarning", "sqrt",
        "stable", "standalone", "start", "state", "statement", "static", "statistics", "stddev_pop", "stddev_samp", "stdin",
        "stdout", "storage", "strict", "strip", "structure", "style", "subclass_origin", "submultiset", "substring",
        "substring_regex", "succeeds", "sum", "symmetric", "sysid", "system", "system_time", "system_user", "t", "table",
        "tables", "tablesample", "tablespace", "table_name", "temp", "template", "temporary", "text", "then", "ties",
        "time", "timestamp", "timezone_hour", "timezone_minute", "to", "token", "top_level_count", "trailing", "transaction",
        "transactions_committed", "transactions_rolled_back", "transaction_active", "transform", "transforms", "translate",
        "translate_regex", "translation", "treat", "trigger", "trigger_catalog", "trigger_name", "trigger_schema", "trim",
        "trim_array", "true", "truncate", "trusted", "type", "types", "uescape", "unbounded", "uncommitted", "under",
        "unencrypted", "union", "unique", "unknown", "unlink", "unlisten", "unlogged", "unnamed", "unnest", "until", "untyped",
        "update", "upper", "uri", "usage", "user", "user_defined_type_catalog", "user_defined_type_code",
        "user_defined_type_name", "user_defined_type_schema", "using", "vacuum", "valid", "validate", "validator", "value",
        "values", "value_of", "varbinary", "varchar", "variadic", "varying", "var_pop", "var_samp", "verbose", "version",
        "versioning", "view", "views", "volatile", "when", "whenever", "where", "whitespace", "width_bucket", "window", "with",
        "within", "without", "work", "wrapper", "write", "xml", "xmlagg", "xmlattributes", "xmlbinary", "xmlcast", "xmlcomment",
        "xmlconcat", "xmldeclaration", "xmldocument", "xmlelement", "xmlexists", "xmlforest", "xmliterate", "xmlnamespaces",
        "xmlparse", "xmlpi", "xmlquery", "xmlroot", "xmlschema", "xmlserialize", "xmltable", "xmltext", "xmlvalidate", "year",
        "yes", "zone",
    };
    // ReSharper restore StringLiteralTypo

    private static readonly HashSet<char> ValidObjectNameChars = "abcdefghijklmnoprstuwxvyz0123456789_".ToHashSet();
  }
}