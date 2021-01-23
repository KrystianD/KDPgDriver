using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using KDPgDriver.Builders;
using KDPgDriver.Fluent;

namespace KDPgDriver
{
  public static class Builders<TModel>
  {
    // ReSharper disable once HeapView.ObjectAllocation.Evident
    public static UpdateStatementsBuilder<TModel> UpdateOp => new UpdateStatementsBuilder<TModel>();

    // Select
    public static SelectQueryFluentBuilder<TModel, TModel> Select()
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectQueryFluentBuilder1Prep<TModel>().Select();
    }

    public static SelectQueryFluentBuilder<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectQueryFluentBuilder1Prep<TModel>().Select(pr);
    }

    public static SelectQueryFluentBuilder<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectQueryFluentBuilder1Prep<TModel>().SelectOnly(builder);
    }

    public static SelectQueryFluentBuilder<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectQueryFluentBuilder1Prep<TModel>().SelectOnly(fieldsList);
    }

    public static SelectQueryFluentBuilder<TModel, bool> Exists()
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectQueryFluentBuilder1Prep<TModel>().Exists();
    }

    // ReSharper disable HeapView.ObjectAllocation.Evident
    public static InsertQueryFluentBuilder1<TModel> Insert() => new InsertQueryFluentBuilder1<TModel>(null);
    public static UpdateQueryFluentBuilder1<TModel> Update() => new UpdateQueryFluentBuilder1<TModel>(null);

    public static DeleteQueryFluentBuilder1<TModel> Delete() => new DeleteQueryFluentBuilder1<TModel>(null);
    // ReSharper restore HeapView.ObjectAllocation.Evident

    public static InsertQueryFluentBuilder1<T> Insert<T>(T obj)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      var b = new InsertQueryFluentBuilder1<T>(null);
      b.AddObject(obj);
      return b;
    }

    public static InsertQueryFluentBuilder1<T> InsertMany<T>(IEnumerable<T> objects)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      var b = new InsertQueryFluentBuilder1<T>(null);
      b.AddMany(objects);
      return b;
    }
  }

  public static class BuildersJoin
  {
    public static SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2> FromMany<TModel1, TModel2>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectMultipleQueryFluentBuilderPrep2<TModel1, TModel2>(null, joinCondition);
    }

    public static SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3> FromMany<TModel1, TModel2, TModel3>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition1,
        Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectMultipleQueryFluentBuilderPrep3<TModel1, TModel2, TModel3>(null, joinCondition1, joinCondition2);
    }

    public static SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4> FromMany<TModel1, TModel2, TModel3, TModel4>(
        Expression<Func<TModel1, TModel2, bool>> joinCondition1,
        Expression<Func<TModel1, TModel2, TModel3, bool>> joinCondition2,
        Expression<Func<TModel1, TModel2, TModel3, TModel4, bool>> joinCondition3)
    {
      // ReSharper disable once HeapView.ObjectAllocation.Evident
      return new SelectMultipleQueryFluentBuilderPrep4<TModel1, TModel2, TModel3, TModel4>(null, joinCondition1, joinCondition2, joinCondition3);
    }
  }
}