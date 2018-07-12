using System;
using System.Linq.Expressions;
using KDPgDriver.Builders;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;

namespace KDPgDriver
{
  public static class Builders<TModel>
  {
    public static QueryBuilder<TModel> Query => new QueryBuilder<TModel>();
    public static InsertQuery<TModel> Insert => new InsertQuery<TModel>();
    public static UpdateStatementsBuilder<TModel> UpdateOp => new UpdateStatementsBuilder<TModel>();

    // Select
    public static SelectQueryFluentBuilder2<TModel, TModel> Select()
    {
      return new SelectQueryFluentBuilder1<TModel>().Select();
    }

    public static SelectQueryFluentBuilder2<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder1<TModel>().Select(pr);
    }

    public static SelectQueryFluentBuilder2<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQueryFluentBuilder1<TModel>().SelectOnly(builder);
    }

    public static SelectQueryFluentBuilder2<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      return new SelectQueryFluentBuilder1<TModel>().SelectOnly(fieldsList);
    }
  }
}