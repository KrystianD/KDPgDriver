using KDPgDriver.Utils;
using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
{
  public class SelectSubqueries
  {
    static SelectSubqueries()
    {
      MyInit.Init();
    }

    [Fact]
    public void SelectSimple()
    {
      var subq = Builders<MyModel>.Select(x => x.Name)
                                  .Where(x => x.Id == 1)
                                  .AsSubquery();

      var q = Builders<MyModel2>.Select(x => x.Name1)
                                .Where(x => x.Name1.PgIn(subq));

      Utils.AssertRawQuery(q, @"
SELECT name1
FROM ""public"".model2
WHERE (name1) IN (
    SELECT ""name"" FROM model
    WHERE (""id"") = (1)
)");
    }

    [Fact]
    public void SelectWithJoin()
    {
      var subq = BuildersJoin.FromMany<MyModel, MyModel2>((m, m2) => m.Id == m2.Id)
                             .Map((m, m2) => new {
                                 M = m,
                                 M2 = m2,
                             })
                             .Select(x => x.M.Name)
                             .Where(x => x.M2.Id == 1)
                             .AsSubquery();

      var q = Builders<MyModel2>.Select(x => x.Name1)
                                .Where(x => x.Name1.PgIn(subq));

      Utils.AssertRawQuery(q, @"
SELECT name1
FROM ""public"".model2
WHERE (name1) IN (
    SELECT t0.""name""
    FROM model t0
    LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.""id""))
    WHERE (t1.""id"") = (1)
)");
    }

    [Fact]
    public void SelectWithJoinNestedSubquery()
    {
      var subq1 = Builders<MyModel>.Select(x => x.Id)
                                   .Where(x => x.Id == 1)
                                   .AsSubquery();

      var subq = BuildersJoin.FromMany<MyModel, MyModel2>((m, m2) => m.Id == m2.Id)
                             .Map((m, m2) => new {
                                 M = m,
                                 M2 = m2,
                             })
                             .Select(x => x.M.Name)
                             .Where(x => x.M2.Id == 1 && x.M.Id.PgIn(subq1))
                             .AsSubquery();

      var q = Builders<MyModel2>.Select(x => x.Name1)
                                .Where(x => x.Name1.PgIn(subq));

      Utils.AssertRawQuery(q, @"
SELECT name1
FROM ""public"".model2
WHERE (name1) IN (
    SELECT t0.""name""
    FROM model t0
    LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.""id""))
    WHERE ((t1.""id"") = (1)) AND ((t0.""id"") IN (SELECT ""id"" FROM model WHERE (""id"") = (1)))
)");
    }

    [Fact]
    public void UpdateSimple()
    {
      var subq = Builders<MyModel>.Select(x => x.Name)
                                  .Where(x => x.Id == 1)
                                  .AsSubquery();

      var q = Builders<MyModel>.Update()
                               .Where(x => x.Name.PgIn(subq))
                               .SetField(x => x.Name, "A");

      Utils.AssertRawQuery(q, @"UPDATE model SET ""name"" = 'A' WHERE (""name"") IN (SELECT ""name"" FROM model WHERE (""id"") = (1))");
    }

    [Fact]
    public void DeleteSimple()
    {
      var subq = Builders<MyModel>.Select(x => x.Name)
                                  .Where(x => x.Id == 1)
                                  .AsSubquery();

      var q = Builders<MyModel>.Delete()
                               .Where(x => x.Name.PgIn(subq));

      Utils.AssertRawQuery(q, @"DELETE FROM model WHERE (""name"") IN (SELECT ""name"" FROM model WHERE (""id"") = (1))");
    }
  }
}