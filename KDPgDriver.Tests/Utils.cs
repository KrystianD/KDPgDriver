using System;
using System.Text.RegularExpressions;
using KDLib;
using KDPgDriver.Builders;
using KDPgDriver.Queries;
using KDPgDriver.Tests.UnitTests;
using KDPgDriver.Utils;
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
        object expected = parameters[i].Value;

        if (expected.GetType().IsArray) {
          var expectedList = ReflectionUtils.CreateListInstance(expected.GetType().GetElementType());
          foreach (var item in (Array) expected) expectedList.Add(item);
          expected = expectedList;
        }

        var actual = list[i].Item1;
        Assert.Equal(expected, actual);
        Assert.Equal(parameters[i].Type, list[i].Item2);
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

      expectedQuery = Regex.Replace(expectedQuery, "[ \n]+", " ").Trim();
      query = Regex.Replace(query, "[ \n]+", " ").Trim();

      Assert.Equal(expectedQuery, query);
      CompareParameters(outParameters, parameters);
    }

    public static void AssertRawQuery(IQuery q, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = q.GetRawQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery q, IQuery q2, string expectedQuery, params Param[] parameters)
    {
      RawQuery rq = q.GetRawQuery(null);
      AssertRawQuery(rq, expectedQuery, parameters);
      AssertRawQuery(q2, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery q, IQuery q2, IQuery q3, string expectedQuery, params Param[] parameters)
    {
      AssertRawQuery(q, expectedQuery, parameters);
      AssertRawQuery(q2, expectedQuery, parameters);
      AssertRawQuery(q3, expectedQuery, parameters);
    }

    // public static void AssertRawQuery<T>(WhereBuilder<T> wb, string expectedQuery, params Param[] parameters)
    // {
    //   RawQuery rq = wb.GetRawQuery();
    //   AssertRawQuery(rq, expectedQuery, parameters);
    // }

    public static void AssertRawQuery(IQuery q, WhereBuilder<MyModel> wb1, string expectedQuery, params Param[] parameters)
    {
      var q2 = Builders<MyModel>.Select(x => new { x.Id }).Where(wb1);
      AssertRawQuery(q, q2, expectedQuery, parameters);
    }

    public static void AssertRawQuery(IQuery q, WhereBuilder<MyModel> wb1, WhereBuilder<MyModel> wb2, string expectedQuery, params Param[] parameters)
    {
      var q2 = Builders<MyModel>.Select(x => new { x.Id }).Where(wb1);
      var q3 = Builders<MyModel>.Select(x => new { x.Id }).Where(wb2);
      AssertRawQuery(q, q2, q3, expectedQuery, parameters);
    }
  }
}