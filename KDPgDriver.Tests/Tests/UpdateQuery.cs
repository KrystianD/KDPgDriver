using System;
using System.Collections.Generic;
using KDPgDriver.Builder;
using KDPgDriver.Utils;
using NpgsqlTypes;
using Xunit;

namespace KDPgDriver.Tests
{
  public class UpdateQuery
  {
    [Fact]
    public void UpdateSetField()
    {
      var q = Builders<MyModel>.Query.Update(b => b.SetField(x => x.Name, "A"));

      Utils.AssertRawQuery(q, @"UPDATE ""public"".""model"" SET ""name"" = 'A'");
    }

    [Fact]
    public void UpdateAddToList()
    {
      var q = Builders<MyModel>.Query.Update(b => b.AddToList(x => x.ListString, "A"));

      Utils.AssertRawQuery(q, @"UPDATE ""public"".""model"" SET ""list_string"" = array_cat(""list_string"", @1::text[])",
                           new Param(new List<string>() { "A" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }

    [Fact]
    public void UpdateRemoveFromList()
    {
      var q = Builders<MyModel>.Query.Update(b => b.RemoveFromList(x => x.ListString, "A"));

      Utils.AssertRawQuery(q, @"UPDATE ""public"".""model"" SET ""list_string"" = array_remove(""list_string"", 'A')");
    }

    [Fact]
    public void UpdateListOperationsCombined()
    {
      var q = Builders<MyModel>.Query.Update(b =>
      {
        b.AddToList(x => x.ListString, "A");
        b.RemoveFromList(x => x.ListString, "B");
        b.AddToList(x => x.ListString2, "C");
      });

      Utils.AssertRawQuery(q, @"UPDATE ""public"".""model"" SET ""list_string"" = array_remove(array_cat(""list_string"", @1::text[]), 'B'), ""list_string2"" = array_cat(""list_string2"", @2::text[])",
                           new Param(new List<string>() { "A" }, NpgsqlDbType.Array | NpgsqlDbType.Text),
                           new Param(new List<string>() { "C" }, NpgsqlDbType.Array | NpgsqlDbType.Text));
    }
  }
}