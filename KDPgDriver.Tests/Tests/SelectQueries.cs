using System;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class SelectQueriesUnitTests
  {
    static SelectQueriesUnitTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void SelectSimple()
    {
      var q = Builders<MyModel>.Query.Select();

      Utils.AssertRawQuery(q, @"SELECT id,name,list_string,list_string2,enum::text,list_enum::text[],enum2::text,datetime FROM public.model");
    }

    [Fact]
    public void SelectSingleValue()
    {
      var q = Builders<MyModel>.Query.Select(x => x.Id);

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model");
    }

    [Fact]
    public void SelectSingleValue2()
    {
      var q = Builders<MyModel>.Query.Select(x => x.Id * 2);

      Utils.AssertRawQuery(q, @"SELECT (id) * (2) FROM public.model");
    }

    [Fact]
    public void SelectColumns()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          x.Name
      });

      Utils.AssertRawQuery(q, @"SELECT name FROM public.model");
    }

    [Fact]
    public void SelectColumnsExpressionString()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          OutName = x.Name + "A" + "B"
      });

      Utils.AssertRawQuery(q, @"SELECT ((name) || ('A')) || ('B') FROM public.model");
    }

    [Fact]
    public void SelectColumnsExpressionNumber()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          OutId = (x.Id + 2 * 3 + 4) * 5,
      });

      Utils.AssertRawQuery(q, @"SELECT (((id) + (6)) + (4)) * (5) FROM public.model");
    }

    [Fact]
    public void SelectColumnsExpressionFunc()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          OutName = x.Name.Substring(5, 10),
      });

      Utils.AssertRawQuery(q, @"SELECT substring((name) from (5) for (10)) FROM public.model");
    }

    [Fact]
    public void SelectColumnsFieldList()
    {
      var fieldsBuilder = new FieldListBuilder<MyModel>();

      fieldsBuilder.AddField(x => x.Id)
                   .AddField(x => x.Name);

      var q = Builders<MyModel>.Query.Select(fieldsBuilder);

      Utils.AssertRawQuery(q, @"SELECT id,name FROM public.model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList1()
    {
      var q = Builders<MyModel>.Query.SelectFields(x => x.Id);

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList2()
    {
      var q = Builders<MyModel>.Query.SelectFields(x => x.Id, x => x.Name);

      Utils.AssertRawQuery(q, @"SELECT id,name FROM public.model");
    }

    [Fact]
    public void SelectEnum()
    {
      MyInit.Init();
      var q = Builders<MyModel>.Query.Select(x => new {
          x.Enum
      });

      Utils.AssertRawQuery(q, @"SELECT (enum)::text FROM public.model");
    }

    [Fact]
    public void SelectEnumArray()
    {
      MyInit.Init();
      var q = Builders<MyModel>.Query.Select(x => new {
          x.ListEnum
      });

      Utils.AssertRawQuery(q, @"SELECT (list_enum)::text[] FROM public.model");
    }
  }
}