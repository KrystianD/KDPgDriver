using System;
using KDLib;
using KDPgDriver.Builder;
using Xunit;

namespace KDPgDriver.Tests
{
  public static class Utils
  {
    public static void CompareParameters(ParametersContainer parametersContainer, params Param[] parameters)
    {
      var list = parametersContainer.GetParametersList();
      Assert.Equal(list.Count, parameters.Length);

      for (var i = 0; i < parameters.Length; i++) {
        object expected = parameters[i].value;

        if (expected.GetType().IsArray) {
          var expectedList = ReflectionUtils.CreateListInstance(expected.GetType().GetElementType());
          foreach (var item in (Array) expected) expectedList.Add(item);
          expected = expectedList;
        }

        var actual = list[i].Item1;
        Assert.Equal(expected, actual);
        Assert.Equal(parameters[i].type, list[i].Item2);
      }
    }

    public static void AssertExpression(TypedExpression exp, string expectedQuery, params Param[] parameters)
    {
      AssertRawQuery(exp.RawQuery, expectedQuery, parameters);
    }

    public static void AssertRawQuery(RawQuery rq, string expectedQuery, params Param[] parameters)
    {
      if (expectedQuery == null) throw new ArgumentNullException(nameof(expectedQuery));

      string query;
      ParametersContainer outParameters;
      rq.Render(out query, out outParameters);

      Assert.Equal(expectedQuery, query.Replace("\n", " ").Trim());
      CompareParameters(outParameters, parameters);
    }

    public static void AssertRawQuery(IQuery gen, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetRawQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(QueryBuilder<MyModel> builder, string expectedQuery, params Param[] parameters)
    {
      var gen = builder.Select(x => new { x.Id });
      RawQuery rq = gen.GetRawQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery gen, RawQuery rq2, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = gen.GetRawQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
      AssertRawQuery(rq2, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery gen, RawQuery rq2, RawQuery rq3, string expectedQuery, params Param[] parameters)
    {
      AssertRawQuery(gen, expectedQuery, parameters);
      AssertRawQuery(rq2, expectedQuery, parameters);
      AssertRawQuery(rq3, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery gen, WhereBuilder<MyModel> b2, string expectedQuery, params Param[] parameters)
    {
      var q2 = new QueryBuilder<MyModel>().Where(b2).Select(x => new { x.Id }).GetRawQuery(null);
      AssertRawQuery(gen, q2, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery gen, WhereBuilder<MyModel> b2, WhereBuilder<MyModel> b3, string expectedQuery, params Param[] parameters)
    {
      var q2 = new QueryBuilder<MyModel>().Where(b2).Select(x => new { x.Id }).GetRawQuery(null);
      var q3 = new QueryBuilder<MyModel>().Where(b3).Select(x => new { x.Id }).GetRawQuery(null);
      AssertRawQuery(gen, q2, q3, expectedQuery, parameters);
    }

    public static void AssertRawQuery<T>(WhereBuilder<T> wb, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = wb.GetRawQuery();
      AssertRawQuery(rq, expectedQuery, parameters);
    }
  }
}