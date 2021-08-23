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
    public void TypeDate()
    {
      var date = DateTime.Parse("2018-01-01 12:34");

      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Date == date);
      Utils.AssertExpression(exp, @"(""date"") = (@1::date)",
                             new Param(date.Date, NpgsqlDbType.Date));

      var q1 = Builders<MyModel>.Insert(new MyModel() { Date = date, })
                                .UseField(x => x.Date);
      Utils.AssertRawQuery(q1, @"INSERT INTO model(""date"") VALUES (@1::date) RETURNING ""id"";",
                           new Param(date.Date, NpgsqlDbType.Date));

      var q2 = Builders<MyModel>.Update()
                                .SetField(x => x.Date, date);
      Utils.AssertRawQuery(q2, @"UPDATE model SET ""date"" = @1::date",
                           new Param(date.Date, NpgsqlDbType.Date));
    }

    [Fact]
    public void TypeTime()
    {
      var date = DateTime.Parse("2018-01-01 12:34");
      
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Time == date.TimeOfDay);
      Utils.AssertExpression(exp, @"(""time"") = (@1::time)",
                             new Param(date.TimeOfDay, NpgsqlDbType.Time));

      var q1 = Builders<MyModel>.Insert(new MyModel() { Time = date.TimeOfDay, })
                                .UseField(x => x.Time);
      Utils.AssertRawQuery(q1, @"INSERT INTO model(""time"") VALUES (@1::time) RETURNING ""id"";",
                           new Param(date.TimeOfDay, NpgsqlDbType.Time));

      var q2 = Builders<MyModel>.Update()
                                .SetField(x => x.Time, date.TimeOfDay);
      Utils.AssertRawQuery(q2, @"UPDATE model SET ""time"" = @1::time",
                           new Param(date.TimeOfDay, NpgsqlDbType.Time));
    }

    [Fact]
    public void TypeDateTime()
    {
      var date = DateTime.Parse("2018-01-01 12:34");
      
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.DateTime == date);
      Utils.AssertExpression(exp, @"(datetime) = (@1::timestamp)",
                             new Param(date, NpgsqlDbType.Timestamp));

      var q1 = Builders<MyModel>.Insert(new MyModel() { DateTime = date, })
                                .UseField(x => x.DateTime);
      Utils.AssertRawQuery(q1, @"INSERT INTO model(datetime) VALUES (@1::timestamp) RETURNING ""id"";",
                           new Param(date, NpgsqlDbType.Timestamp));

      var q2 = Builders<MyModel>.Update()
                                .SetField(x => x.DateTime, date);
      Utils.AssertRawQuery(q2, @"UPDATE model SET datetime = @1::timestamp",
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

    [Fact]
    public void TypeEnum()
    {
      var exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.Enum == MyEnum.A);
      Utils.AssertExpression(exp, @"(""enum"") = ('A')");

      exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.ListEnum.Contains(MyEnum.A));
      Utils.AssertExpression(exp, @"('A') = ANY((list_enum))");

      exp = NodeVisitor.VisitFuncExpression<MyModel>(x => x.EnumText == MyEnumText.TextEnum2);
      Utils.AssertExpression(exp, @"(enum_text) = ('TextEnum2')");
    }
  }
}