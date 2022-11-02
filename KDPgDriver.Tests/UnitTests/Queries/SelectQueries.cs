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
      var q = Builders<MyModel>.Select();

      Utils.AssertRawQuery(q, @"SELECT <COLUMNS:MyModel> FROM model");
    }

    // Single values
    [Fact]
    public void SelectSingleValue()
    {
      var q = Builders<MyModel>.Select(x => x.Id);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model");
    }

    [Fact]
    public void SelectSingleValue2()
    {
      var q = Builders<MyModel>.Select(x => x.Id * 2);

      Utils.AssertRawQuery(q, @"SELECT (""id"") * (2) FROM model");
    }

    // Columns
    [Fact]
    public void SelectColumns()
    {
      var q = Builders<MyModel>.Select(x => new {
          x.Name
      });

      Utils.AssertRawQuery(q, @"SELECT ""name"" FROM model");
    }

    [Fact]
    public void SelectColumnsExpressionString()
    {
      var q = Builders<MyModel>.Select(x => new {
          OutName = x.Name + "A" + "B"
      });

      Utils.AssertRawQuery(q, @"SELECT ((""name"") || ('A')) || ('B') FROM model");
    }

    private class CustomDto
    {
      public MyModel M1 { get; set; }
      public int Id { get; set; }
    }

    [Fact]
    public void SelectCustomDto()
    {
      var q = Builders<MyModel>.Select(x => new CustomDto {
          Id = x.Id * 2,
          M1 = x,
      });

      Utils.AssertRawQuery(q, @"SELECT (""id"") * (2),<COLUMNS:MyModel> FROM model");
    }

    [Fact]
    public void SelectColumnsFieldList()
    {
      var fieldsBuilder = new FieldListBuilder<MyModel>();

      fieldsBuilder.AddField(x => x.Id)
                   .AddField(x => x.Name) // duplicated on purpose
                   .AddField(x => x.Name);

      var q = Builders<MyModel>.SelectOnly(fieldsBuilder);

      Utils.AssertRawQuery(q, @"SELECT ""id"",""name"" FROM model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList1()
    {
      var q = Builders<MyModel>.SelectOnly(x => x.Id);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model");
    }

    [Fact]
    public void SelectColumnsDirectFieldList2()
    {
      var q = Builders<MyModel>.SelectOnly(x => x.Id, x => x.Name);

      Utils.AssertRawQuery(q, @"SELECT ""id"",""name"" FROM model");
    }

    // Exists
    [Fact]
    public void Exists()
    {
      var q = Builders<MyModel>.Exists();

      Utils.AssertRawQuery(q, @"SELECT EXISTS(SELECT 1 FROM model)");
    }

    // Enums
    [Fact]
    public void SelectEnum()
    {
      var q = Builders<MyModel>.Select(x => new {
          x.Enum
      });

      Utils.AssertRawQuery(q, @"SELECT (""enum"")::text FROM model");
    }

    [Fact]
    public void SelectEnumArray()
    {
      var q = Builders<MyModel>.Select(x => new {
          x.ListEnum
      });

      Utils.AssertRawQuery(q, @"SELECT (list_enum)::text[] FROM model");
    }

    // Order
    [Fact]
    public void SelectOrderBy()
    {
      var q = Builders<MyModel>.Select(x => x.Id)
                               .OrderBy(x => x.Id + 2)
                               .OrderByDescending(x => x.DateTime);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model ORDER BY (""id"") + (2),datetime DESC");
    }

    // Limit
    [Fact]
    public void SelectLimit()
    {
      var q = Builders<MyModel>.Select(x => x.Id)
                               .Limit(1)
                               .Offset(2);

      Utils.AssertRawQuery(q, @"SELECT ""id"" FROM model LIMIT 1 OFFSET 2");
    }

    // Distinct
    [Fact]
    public void SelectDistinct()
    {
      var q = Builders<MyModel>.Select(x => x.Id)
                               .Distinct();

      Utils.AssertRawQuery(q, @"SELECT DISTINCT ""id"" FROM model");
    }
  }
}