using Xunit;

namespace KDPgDriver.Tests.UnitTests.Queries
{
  public class SelectMultipleQueriesUnitTests
  {
    static SelectMultipleQueriesUnitTests()
    {
      MyInit.Init();
    }

    [Fact]
    public void SelectMultipleAll()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select()
                          .Where(x => x.M2 != null && x.M1.Name == "Q");

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 
  t0 IS NULL,t0.""id"",t0.""name"",t0.list_string,t0.list_string2,(t0.""enum"")::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.enum_text,t0.""date"",t0.""time"",t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t0.bool,t0.""binary"",t0.private_int,t0.val_f32,t0.val_f64,t1 IS NULL,t1.""id"",t1.name1,t1.model_id
FROM
  model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id))
WHERE 
  (NOT((t1) IS NULL)) AND ((t0.""name"") = ('Q'))");
    }

    [Fact]
    public void SelectMultipleNew()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(x => new {
                              M1 = x.M1,
                              M2_name = x.M2.Name1,
                              M3_calc = x.M2.Id * 2,
                          });

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 
 t0 IS NULL,t0.""id"",t0.""name"",t0.list_string,t0.list_string2,(t0.""enum"")::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.enum_text,t0.""date"",t0.""time"",t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t0.bool,t0.""binary"",t0.private_int,t0.val_f32,t0.val_f64,t1.name1,(t1.""id"") * (2)
FROM
  model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id))");
    }

    private class JoinClass
    {
      public MyModel M1 { get; set; }
      public MyModel2 M2 { get; set; }
    }

    [Fact]
    public void SelectMultipleNewClass()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.ModelId)
                          .Map((a, b) => new JoinClass {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(x => new {
                              M1_name = x.M1.Name,
                              M2_name = x.M2.Name1,
                          });

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 
  t0.""name"",t1.name1
FROM
  model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id))");
    }

    [Fact]
    public void SelectMultiple3Tables()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2, MyModel3>(
                              (a, b) => a.Id == b.ModelId,
                              (a, b, c) => a.Id == c.ModelId)
                          .Map((a, b, c) => new {
                              T0 = a,
                              T1 = b,
                              T2 = c,
                          })
                          .Select(x => new {
                              x.T0.Id,
                              x.T1.Name1,
                          });

      Utils.AssertRawQueryWithAliases(q, @"SELECT t0.""id"",t1.name1 FROM model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id)) LEFT JOIN ""public"".model3 t2 ON ((t0.""id"") = (t2.model_id))");

      var q2 = BuildersJoin.FromMany<MyModel, MyModel2, MyModel3>(
                               (a, b) => a.Id == b.ModelId,
                               (a, b, c) => a.Id == c.ModelId)
                           .Map((a, b, c) => new {
                               T0 = b,
                               T1 = a,
                               T2 = c,
                           })
                           .Select(x => new {
                               x.T0.Id,
                               x.T1.Name,
                           });

      Utils.AssertRawQueryWithAliases(q2, @"SELECT t1.""id"",t0.""name"" FROM model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id)) LEFT JOIN ""public"".model3 t2 ON ((t0.""id"") = (t2.model_id))");
    }

    [Fact]
    public void SelectMultiple4Tables()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel2, MyModel3, MyModel3>(
                              (a, b) => a.Id == b.ModelId,
                              (a, b, c) => a.Id == c.ModelId,
                              (a, b, c, d) => a.Id == d.ModelId)
                          .Map((a, b, c, d) => new {
                              T0 = a,
                              T1 = b,
                              T2 = c,
                              T3 = d,
                          })
                          .Select(x => 0);

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 0
FROM model t0
LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.model_id))
LEFT JOIN ""public"".model3 t2 ON ((t0.""id"") = (t2.model_id))
LEFT JOIN ""public"".model3 t3 ON ((t0.""id"") = (t3.model_id))
");
    }

    [Fact]
    public void SelectMultipleSelfJoin()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel>((a, b) => a.Id == b.Id)
                          .Map((a, b) => new {
                              M1 = b,
                              M2 = a,
                          })
                          .Select(x => new {
                              A = x.M1,
                              B = x.M2,
                          })
                          .Where(x => x.M1 != null && x.M2 != null);

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 
  t1 IS NULL,t1.""id"",t1.""name"",t1.list_string,t1.list_string2,(t1.""enum"")::text,(t1.list_enum)::text[],(t1.enum2)::text,t1.enum_text,t1.""date"",t1.""time"",t1.datetime,t1.json_object1,t1.json_model,t1.json_array1,t1.bool,t1.""binary"",t1.private_int,t1.val_f32,t1.val_f64,t0 IS NULL,t0.""id"",t0.""name"",t0.list_string,t0.list_string2,(t0.""enum"")::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.enum_text,t0.""date"",t0.""time"",t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t0.bool,t0.""binary"",t0.private_int,t0.val_f32,t0.val_f64
FROM
  model t0 LEFT JOIN model t1 ON ((t0.""id"") = (t1.""id""))
WHERE 
  (NOT((t1) IS NULL)) AND (NOT((t0) IS NULL))");
    }

    [Fact]
    public void SelectMultipleReturnSingleTable()
    {
      var q = BuildersJoin.FromMany<MyModel, MyModel>((a, b) => a.Id == b.Id)
                          .Map((a, b) => new {
                              M1 = a,
                              M2 = b,
                          })
                          .Select(x => x.M1);

      Utils.AssertRawQueryWithAliases(q, @"
SELECT 
  t0 IS NULL,t0.""id"",t0.""name"",t0.list_string,t0.list_string2,(t0.""enum"")::text,(t0.list_enum)::text[],(t0.enum2)::text,t0.enum_text,t0.""date"",t0.""time"",t0.datetime,t0.json_object1,t0.json_model,t0.json_array1,t0.bool,t0.""binary"",t0.private_int,t0.val_f32,t0.val_f64
FROM
  model t0 LEFT JOIN model t1 ON ((t0.""id"") = (t1.""id""))");
    }

    [Fact]
    public void SelectMultipleReturnScalar()
    {
      var q1 = BuildersJoin.FromMany<MyModel, MyModel2>((a, b) => a.Id == b.Id)
                           .Map((a, b) => new {
                               M1 = a,
                               M2 = b,
                           })
                           .Select(x => x.M1.Id);

      Utils.AssertRawQueryWithAliases(q1, @"
SELECT 
  t0.""id""
FROM
  model t0 LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.""id""))");

      var q2 = BuildersJoin.FromMany<MyModel, MyModel2, MyModel3>(
                               (a, b) => a.Id == b.Id,
                               (a, b, c) => a.Id == c.Id)
                           .Map((a, b, c) => new {
                               M1 = a,
                           })
                           .Select(x => x.M1.Id);

      Utils.AssertRawQueryWithAliases(q2, @"
SELECT 
  t0.""id""
FROM
  model t0
LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.""id""))
LEFT JOIN ""public"".model3 t2 ON ((t0.""id"") = (t2.""id""))");

      var q3 = BuildersJoin.FromMany<MyModel, MyModel2, MyModel3, MyModel2>(
                               (a, b) => a.Id == b.Id,
                               (a, b, c) => a.Id == c.Id,
                               (a, b, c, d) => a.Id == d.Id)
                           .Map((a, b, c, d) => new {
                               M1 = a,
                           })
                           .Select(x => x.M1.Id);

      Utils.AssertRawQueryWithAliases(q3, @"
SELECT 
  t0.""id""
FROM
  model t0
LEFT JOIN ""public"".model2 t1 ON ((t0.""id"") = (t1.""id""))
LEFT JOIN ""public"".model3 t2 ON ((t0.""id"") = (t2.""id""))
LEFT JOIN ""public"".model2 t3 ON ((t0.""id"") = (t3.""id""))");
    }
  }
}