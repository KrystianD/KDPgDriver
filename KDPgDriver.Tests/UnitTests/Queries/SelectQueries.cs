using KDPgDriver.Builders;
using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
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

      Utils.AssertRawQuery(q, @"SELECT id,name,list_string,list_string2,(enum)::text,(list_enum)::text[],(enum2)::text,datetime,json_object1,json_model FROM public.model");
    }

    // Single values
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

    // Columns
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
    public void SelectColumnsFieldList()
    {
      var fieldsBuilder = new FieldListBuilder<MyModel>();

      fieldsBuilder.AddField(x => x.Id)
                   .AddField(x => x.Name);

      var q = Builders<MyModel>.Query.SelectOnly(fieldsBuilder);

      Utils.AssertRawQuery(q, @"SELECT id,name FROM public.model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList1()
    {
      var q = Builders<MyModel>.Query.SelectOnly(x => x.Id);

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList2()
    {
      var q = Builders<MyModel>.Query.SelectOnly(x => x.Id, x => x.Name);

      Utils.AssertRawQuery(q, @"SELECT id,name FROM public.model");
    }

    // Enums
    [Fact]
    public void SelectEnum()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          x.Enum
      });

      Utils.AssertRawQuery(q, @"SELECT (enum)::text FROM public.model");
    }

    [Fact]
    public void SelectEnumArray()
    {
      var q = Builders<MyModel>.Query.Select(x => new {
          x.ListEnum
      });

      Utils.AssertRawQuery(q, @"SELECT (list_enum)::text[] FROM public.model");
    }

    // Order
    [Fact]
    public void SelectOrderBy()
    {
      var q = Builders<MyModel>.Select(x => x.Id)
                               .OrderBy(x => x.Id + 2)
                               .OrderByDescending(x => x.DateTime);

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model ORDER BY (id) + (2),datetime DESC");
    }

    // Limit
    [Fact]
    public void SelectLimit()
    {
      var q = Builders<MyModel>.Select(x => x.Id)
                               .Limit(1)
                               .Offset(2);

      Utils.AssertRawQuery(q, @"SELECT id FROM public.model LIMIT 1 OFFSET 2");
    }
  }
}