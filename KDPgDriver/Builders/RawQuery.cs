using System.Collections.Generic;
using System.Linq;
using System.Text;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class RawQuery
  {
    private class QueryPart
    {
      public StringBuilder Text;
      public int ParamIdx = -1;
      public RawQuery RawQuery;
    }

    private List<QueryPart> parts = new List<QueryPart>();
    private List<Helper.PgValue> parameters = new List<Helper.PgValue>();

    public bool IsEmpty => parts.Count == 0;

    public RawQuery Append(string text)
    {
      if (parts.Count > 0) {
        var last = parts.Last();
        if (last.Text != null) {
          last.Text.Append(text);
          return this;
        }
      }

      parts.Add(new QueryPart() { Text = new StringBuilder(text) });
      return this;
    }

    public RawQuery Append(params string[] texts)
    {
      foreach (var text in texts)
        Append(text);
      return this;
    }

    public RawQuery Append(Helper.PgValue value)
    {
      if (ParametersContainer.TryInline(value, out string inlined)) {
        Append(inlined);
      }
      else {
        int idx = parameters.Count;
        parameters.Add(value);
        parts.Add(new QueryPart() {
            ParamIdx = idx,
        });
      }

      return this;
    }

    public RawQuery Append(RawQuery rawQuery)
    {
      parts.Add(new QueryPart() {
          RawQuery = rawQuery,
      });
      return this;
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

    public RawQuery AppendSurround(Helper.PgValue value)
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

    public RawQuery AppendTableName(string tableName, string schema = null)
    {
      if (schema == null)
        Append(Helper.QuoteObjectName(tableName));
      else
        Append($"{Helper.QuoteObjectName(schema)}.{Helper.QuoteObjectName(tableName)}");
      return this;
    }

    public RawQuery AppendColumnName(string columnName)
    {
      Append(Helper.QuoteObjectName(columnName));
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

    public RawQuery AppendColumnNameWithCast(string columnName, string castType = null)
    {
      AppendColumnName(columnName);
      if (castType != null)
        Append("::", castType);

      return this;
    }

    public RawQuery AppendWithCast(string columnName, string castType = null)
    {
      if (castType == null) {
        Append(columnName);
      }
      else {
        AppendSurround(columnName);
        Append("::", castType);
      }

      return this;
    }

    public void Render(out string query, out ParametersContainer outParameters)
    {
      outParameters = new ParametersContainer();
      query = RenderInto(outParameters);
    }

    public string RenderInto(ParametersContainer outParameters)
    {
      var sb = new StringBuilder();

      foreach (var part in parts) {
        if (part.Text != null) {
          sb.Append(part.Text);
        }

        if (part.ParamIdx != -1) {
          sb.Append(outParameters.GetNextParam(parameters[part.ParamIdx]));
        }

        if (part.RawQuery != null) {
          sb.Append(part.RawQuery.RenderInto(outParameters));
        }
      }

      return sb.ToString();
    }

    public string RenderSimple()
    {
      var sb = new StringBuilder();

      foreach (var part in parts) {
        if (part.Text != null)
          sb.Append(part.Text);

        else if (part.ParamIdx != -1)
          sb.Append(Helper.EscapePostgresValue(parameters[part.ParamIdx].Value));

        else if (part.RawQuery != null)
          sb.Append(part.RawQuery.RenderSimple());
      }

      return sb.ToString();
    }

    public override string ToString()
    {
      string query;
      Render(out query, out _);
      return query;
    }

    // static creationg
    public static RawQuery Create(params string[] texts)
    {
      var rq = new RawQuery();
      rq.Append(texts);
      return rq;
    }

    public static RawQuery Create(Helper.PgValue value)
    {
      var rq = new RawQuery();
      rq.Append(value);
      return rq;
    }

    public static RawQuery CreateColumnName(string name)
    {
      var rq = new RawQuery();
      rq.AppendColumnName(name);
      return rq;
    }
  }
}