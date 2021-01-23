using System;
using KDPgDriver.Traverser;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests.UnitTests
{
  public class DataTypesTests
  {
    static DataTypesTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void TypeBool()
    {
      var exp1 = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Bool == false);

      Utils.AssertExpression(exp1, @"(bool) = (FALSE)");

      var exp2 = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Bool);

      Utils.AssertExpression(exp2, @"bool");
    }

    [Fact]
    public void TypeDateTime()
    {
      var date = DateTime.Parse("2018-01-01 12:34");
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.DateTime == date);

      Utils.AssertExpression(exp, @"(datetime) = (@1::timestamp)",
                             new Param(date, NpgsqlDbType.Timestamp));
    }

    [Fact]
    public void TypeBinary()
    {
      var data = new byte[] { 1, 2, 3 };
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Binary == data);

      Utils.AssertExpression(exp, @"(""binary"") = (@1::bytea)",
                             new Param(data, NpgsqlDbType.Bytea));
    }
  }
}