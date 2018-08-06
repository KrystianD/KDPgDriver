﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection.Metadata.Ecma335;
using KDPgDriver.Builders;
using KDPgDriver.Fluent;
using KDPgDriver.Queries;

namespace KDPgDriver
{
  public static class Builders<TModel>
  {
    // public static QueryBuilder<TModel> Query => new QueryBuilder<TModel>();
    // public static InsertQuery<TModel> Insert => new InsertQuery<TModel>();
    public static UpdateStatementsBuilder<TModel> UpdateOp => new UpdateStatementsBuilder<TModel>();

    // Select
    public static SelectQueryFluentBuilder1<TModel, TModel> Select()
    {
      return new SelectQueryFluentBuilder1Prep<TModel>().Select();
    }

    public static SelectQueryFluentBuilder1<TModel, TNewModel> Select<TNewModel>(Expression<Func<TModel, TNewModel>> pr)
    {
      return new SelectQueryFluentBuilder1Prep<TModel>().Select(pr);
    }

    public static SelectQueryFluentBuilder1<TModel, TModel> SelectOnly(FieldListBuilder<TModel> builder)
    {
      return new SelectQueryFluentBuilder1Prep<TModel>().SelectOnly(builder);
    }

    public static SelectQueryFluentBuilder1<TModel, TModel> SelectOnly(params Expression<Func<TModel, object>>[] fieldsList)
    {
      return new SelectQueryFluentBuilder1Prep<TModel>().SelectOnly(fieldsList);
    }

    public static InsertQueryFluentBuilder1<TModel> Insert() => new InsertQueryFluentBuilder1<TModel>(null);
    public static UpdateQueryFluentBuilder1<TModel> Update() => new UpdateQueryFluentBuilder1<TModel>(null);
    public static DeleteQueryFluentBuilder1<TModel> Delete() => new DeleteQueryFluentBuilder1<TModel>(null);

    public static InsertQueryFluentBuilder1<T> Insert<T>(T obj)
    {
      var b = new InsertQueryFluentBuilder1<T>(null);
      b.AddObject(obj);
      return b;
    }

    public static InsertQueryFluentBuilder1<T> InsertMany<T>(IEnumerable<T> objects)
    {
      var b = new InsertQueryFluentBuilder1<T>(null);
      b.AddMany(objects);
      return b;
    }
  }
}