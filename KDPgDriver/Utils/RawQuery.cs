using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using JetBrains.Annotations;

namespace KDPgDriver.Utils
{
  [PublicAPI]
  public class RawQuery
  {
    private class QueryPart
    {
      public StringBuilder Text;
      public int ParamIdx = -1;

      public RawQuery RawQuery;

      public ColumnPart Column;
      public TableNamePlaceholder Table;
    }

    public class TableNamePlaceholder
    {
      public KdPgTableDescriptor Table;
      public string Name;

      public TableNamePlaceholder(KdPgTableDescriptor table, string name)
      {
        Table = table;
        Name = name;
      }
    }

    public class ColumnPart
    {
      public KdPgColumnDescriptor Column;
      public TableNamePlaceholder TablePlaceholder;

      public ColumnPart(KdPgColumnDescriptor column, TableNamePlaceholder tablePlaceholder)
      {
        Column = column;
        TablePlaceholder = tablePlaceholder;
      }
    }


    private class RenderingContext
    {
      public Stack<Dictionary<string, string>> AliasesStack;
      public bool SkipExplicitColumnTableNames;
    }

    private bool _isSimple;
    private readonly List<QueryPart> _parts = new List<QueryPart>();
    private readonly List<PgValue> _parameters = new List<PgValue>();

    public bool IsEmpty => _parts.Count == 0;

    private readonly Dictionary<string, string> _aliases = new Dictionary<string, string>();
    private bool _skipExplicitColumnTableNames;

    public void ApplyAlias(string table, string newAlias)
    {
      _aliases[table] = newAlias;
    }

    public RawQuery MarkSimple()
    {
      _isSimple = true;
      return this;
    }

    #region Append

    public RawQuery Append(string text)
    {
      if (_parts.Count > 0) {
        var last = _parts.Last();
        if (last.Text != null) {
          last.Text.Append(text);
          return this;
        }
      }

      _parts.Add(new QueryPart() { Text = new StringBuilder(text) });
      return this;
    }

    public RawQuery Append(params string[] texts)
    {
      foreach (var text in texts)
        Append(text);
      return this;
    }

    public RawQuery Append(PgValue value)
    {
      if (ParametersContainer.TryInlineShortValue(value, out string inlined)) {
        Append(inlined);
      }
      else {
        int idx = _parameters.Count;
        _parameters.Add(value);
        _parts.Add(new QueryPart() {
            ParamIdx = idx,
        });
      }

      return this;
    }

    public RawQuery Append(RawQuery rawQuery)
    {
      _parts.Add(new QueryPart() {
          RawQuery = rawQuery,
      });
      return this;
    }

    public RawQuery AppendFuncInvocation(string funcName, RawQuery rawQuery)
    {
      Append(funcName);
      Append("(");
      Append(rawQuery);
      Append(")");
      return this;
    }

    public RawQuery AppendTable(TableNamePlaceholder alias)
    {
      _parts.Add(new QueryPart() {
          Table = alias,
      });
      return this;
    }

    public RawQuery AppendColumn(KdPgColumnDescriptor column, TableNamePlaceholder tableAlias)
    {
      Debug.Assert(tableAlias != null);
      _parts.Add(new QueryPart() {
          Column = new ColumnPart(column, tableAlias),
      });
      return this;
    }

    public void AppendSeparatorIfNotEmpty()
    {
      if (!IsEmpty)
        Append(",");
    }

    public RawQuery AppendSurround(string text)
    {
      Append("(");
      Append(text);
      Append(")");
      return this;
    }

    public RawQuery AppendSurround(params string[] texts)
    {
      Append("(");
      Append(texts);
      Append(")");
      return this;
    }

    public RawQuery AppendSurround(PgValue value)
    {
      if (value.Value == null) {
        Append("NULL");
      }
      else {
        Append("(");
        Append(value);
        Append(")");
      }

      return this;
    }

    public RawQuery AppendSurround(RawQuery rawQuery)
    {
      Append("(");
      Append(rawQuery);
      Append(")");
      return this;
    }

    public RawQuery AppendTableName(string tableName, string schema = null, string alias = null)
    {
      if (schema == null)
        Append(EscapeUtils.QuoteObjectName(tableName));
      else
        Append($"{EscapeUtils.QuoteObjectName(schema)}.{EscapeUtils.QuoteObjectName(tableName)}");

      if (alias != null)
        Append(" ", EscapeUtils.QuoteObjectName(alias));

      return this;
    }

    public RawQuery AppendColumnName(string columnName)
    {
      Append(EscapeUtils.QuoteObjectName(columnName));
      return this;
    }

    public RawQuery AppendObjectName(string name)
    {
      Append(EscapeUtils.QuoteObjectName(name));
      return this;
    }

    public RawQuery AppendColumnNames(IEnumerable<string> columnNames)
    {
      bool first = true;
      foreach (var columnName in columnNames) {
        if (!first)
          Append(",");
        AppendColumnName(columnName);
        first = false;
      }

      return this;
    }

    public RawQuery AppendWithCast(RawQuery query, string castType = null)
    {
      RawQuery q = new RawQuery();

      if (castType == null) {
        q.Append(query);
      }
      else {
        q.Append("(");
        q.Append(query);
        q.Append(")");
        q.Append("::", castType);
      }

      Append(q);

      return this;
    }

    #endregion

    public void Render(out string query, out ParametersContainer outParameters)
    {
      var ctx = new RenderingContext();
      ctx.AliasesStack = new Stack<Dictionary<string, string>>();
      ctx.AliasesStack.Push(_aliases);
      ctx.SkipExplicitColumnTableNames = _skipExplicitColumnTableNames;
      outParameters = new ParametersContainer();
      query = RenderInto(outParameters, ctx);
    }


    private string ResolvePlaceholder(RenderingContext ctx, TableNamePlaceholder placeholder)
    {
      var alias = ctx.AliasesStack.Select(x => x.GetValueOrDefault(placeholder.Name)).FirstOrDefault(x => x != null) ?? placeholder.Name;

      if (alias == placeholder.Table.Name)
        return null;

      return alias;
    }

    private string RenderInto(ParametersContainer outParameters, RenderingContext ctx)
    {
      var sb = new StringBuilder();

      ctx.AliasesStack.Push(_aliases);

      if (_isSimple) {
        var res = RenderSimple(ctx);
        ctx.AliasesStack.Pop();
        return res;
      }

      foreach (var part in _parts) {
        if (part.Text != null)
          sb.Append(part.Text);

        if (part.Table != null) {
          var alias = ResolvePlaceholder(ctx, part.Table);
          if (alias != null)
            sb.Append(EscapeUtils.QuoteObjectName(alias));
          else
            sb.Append(EscapeUtils.QuoteTable(part.Table.Table.Name, part.Table.Table.Schema));
        }

        if (part.Column != null) {
          var alias = ResolvePlaceholder(ctx, part.Column.TablePlaceholder);
          if (alias != null) {
            sb.Append(EscapeUtils.QuoteObjectName(alias));
            sb.Append(".");
          }

          sb.Append(EscapeUtils.QuoteObjectName(part.Column.Column.Name));
        }

        if (part.ParamIdx != -1)
          sb.Append(outParameters.GetNextParam(_parameters[part.ParamIdx]));

        if (part.RawQuery != null)
          sb.Append(part.RawQuery.RenderInto(outParameters, ctx));
      }

      ctx.AliasesStack.Pop();
      return sb.ToString();
    }

    private string RenderSimple(RenderingContext ctx)
    {
      var sb = new StringBuilder();

      ctx.AliasesStack.Push(_aliases);

      foreach (var part in _parts) {
        if (part.Text != null)
          sb.Append(part.Text);

        if (part.Table != null) {
          var alias = ResolvePlaceholder(ctx, part.Table);
          if (alias != null)
            sb.Append(EscapeUtils.QuoteObjectName(alias));
          else
            sb.Append(EscapeUtils.QuoteTable(part.Table.Table.Name, part.Table.Table.Schema));
        }

        if (part.Column != null) {
          // if (!ctx.skipExplicitColumnTableNames) {
          var alias = ResolvePlaceholder(ctx, part.Column.TablePlaceholder);
          if (alias != null) {
            sb.Append(EscapeUtils.QuoteObjectName(alias));
            sb.Append(".");
          }

          sb.Append(EscapeUtils.QuoteObjectName(part.Column.Column.Name));
        }

        else if (part.ParamIdx != -1)
          sb.Append(EscapeUtils.EscapePostgresValue(_parameters[part.ParamIdx].Value));

        else if (part.RawQuery != null)
          sb.Append(part.RawQuery.RenderSimple(ctx));
      }

      ctx.AliasesStack.Pop();
      return sb.ToString();
    }

    public override string ToString()
    {
      string query;
      Render(out query, out _);
      return query;
    }

    // static creation
    public static RawQuery Empty => new RawQuery();

    public static RawQuery Create(params string[] texts)
    {
      var rq = new RawQuery();
      rq.Append(texts);
      return rq;
    }

    public static RawQuery Create(PgValue value)
    {
      var rq = new RawQuery();
      rq.Append(value);
      return rq;
    }

    // public static RawQuery CreateColumn(KdPgColumnDescriptor column)
    // {
    //   var rq = new RawQuery();
    //   rq.AppendColumn(column);
    //   return rq;
    // }

    // public static RawQuery CreateTable(KdPgTableDescriptor table)
    // {
    //   var rq = new RawQuery();
    //   rq.AppendTable(table);
    //   return rq;
    // }
    public void SkipExplicitColumnTableNames()
    {
      _skipExplicitColumnTableNames = true;
    }
  }
}