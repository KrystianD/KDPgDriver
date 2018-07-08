using System;
using KDPgDriver.Builder;
using Xunit;

namespace KDPgDriver.Tests
{
  public class Utils
  {
    public static void CompareParameters(ParametersContainer parametersContainer, params Param[] parameters)
    {
      var list = parametersContainer.GetParametersList();
      Assert.Equal(list.Count, parameters.Length);

      for (var i = 0; i < parameters.Length; i++) {
        Assert.Equal(parameters[i].value, list[i].Item1);
        Assert.Equal(parameters[i].type, list[i].Item2);
      }
    }

    public static void AssertRawQuery(RawQuery rq, string expectedQuery, params Param[] parameters)
    {
      if (expectedQuery == null) throw new ArgumentNullException(nameof(expectedQuery));
      
      string query;
      ParametersContainer outParameters;
      rq.Render(out query, out outParameters);

      Assert.Equal(expectedQuery, query.Replace("\n", " "));
      CompareParameters(outParameters, parameters);
    }

    public static void AssertRawQuery(ISelectQuery gen, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IInsertQuery gen, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IUpdateQuery gen, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(QueryBuilder<MyModel> builder, string expectedQuery, params Param[] parameters)
    {
      var gen = builder.Select(x => new { x.Id });
      RawQuery rq = gen.GetQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(ISelectQuery gen, RawQuery rq2, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
      AssertRawQuery(rq2, expectedQuery, parameters);
    }

    public static void AssertRawQuery(ISelectQuery gen, WhereBuilder<MyModel> b2, string expectedQuery, params Param[] parameters)
    {
      var q2 = new QueryBuilder<MyModel>().Where(b2).Select(x => new { x.Id }).GetQuery(null);
      AssertRawQuery(gen, q2, expectedQuery, parameters);
    }
  }
}