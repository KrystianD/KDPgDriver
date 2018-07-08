using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Text;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public class RawQuery
  {
    private class QueryPart
    {
      public string text = null;
      public int paramIdx = -1;
      public RawQuery rawQuery = null;
    }

    private List<QueryPart> parts = new List<QueryPart>();
    private List<Helper.PgValue> parameters = new List<Helper.PgValue>();

    public bool IsEmpty => parts.Count == 0;

    public RawQuery Append(string text)
    {
      parts.Add(new QueryPart() { text = text, });
      return this;
    }

    public RawQuery Append(params string[] texts)
    {
      foreach (var text in texts)
        parts.Add(new QueryPart() { text = text, });

      return this;
    }

    public RawQuery Append(Helper.PgValue value)
    {
      int idx = parameters.Count;
      parameters.Add(value);
      parts.Add(new QueryPart()
      {
          paramIdx = idx,
      });
      return this;
    }

    public RawQuery Append(RawQuery rawQuery)
    {
      parts.Add(new QueryPart()
      {
          rawQuery = rawQuery,
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

    public void Render(out string query, out ParametersContainer outParameters)
    {
      outParameters = new ParametersContainer();
      query = RenderInto(outParameters);
    }

    public string RenderInto(ParametersContainer outParameters)
    {
      var sb = new StringBuilder();

      foreach (var part in parts) {
        if (part.text != null) {
          sb.Append(part.text);
        }

        if (part.paramIdx != -1) {
          sb.Append(outParameters.GetNextParam(parameters[part.paramIdx]));
        }

        if (part.rawQuery != null) {
          sb.Append(part.rawQuery.RenderInto(outParameters));
        }
      }

      return sb.ToString();
    }

    public string RenderSimple()
    {
      var sb = new StringBuilder();

      foreach (var part in parts) {
        if (part.text != null) {
          sb.Append(part.text);
        }

        if (part.paramIdx != -1) {
          var v = parameters[part.paramIdx].Value;
          sb.Append(Helper.EscapePostgresValue(v));
        }

        if (part.rawQuery != null) {
          sb.Append(part.rawQuery.RenderSimple());
        }
      }

      return sb.ToString();

      // if (parameters.Count > 0)
      //   throw new Exception("simple raw queries can't have parameters");
      // string query;
      // ParametersContainer outParameters;
      // Render(out query, out outParameters);
      // return query;
    }

    public override string ToString()
    {
      string query;
      Render(out query, out _);
      return query;
    }

    // operator
    public static implicit operator RawQuery(string text)
    {
      return Create(text);
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
      rq.Append($"\"{name}\"");
      return rq;
    }
  }
}