using System;
using System.Linq.Expressions;
using KDPgDriver.Builders;

namespace KDPgDriver
{
  public static class Builders<T>
  {
    public static QueryBuilder<T> Query => new QueryBuilder<T>();
    public static InsertQuery<T> Insert => new InsertQuery<T>();
    public static UpdateStatementsBuilder<T> UpdateOp => new UpdateStatementsBuilder<T>();
    
    public static SelectQueryFluentBuilder1<T> From()
    {
      return new SelectQueryFluentBuilder1<T>(null);
    }
    
    
    
    

    public static SelectQueryFluentBuilder2<T, T> Select()
    {
      return new SelectQueryFluentBuilder2<T, T>(null, SelectFromBuilder<T>.AllColumns());
    }

    public static SelectQueryFluentBuilder2<T, TNewModel> Select<TNewModel>(Expression<Func<T, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder2<T, TNewModel>(null, SelectFromBuilder<TNewModel>.FromExpression(pr));
    }
  }
}