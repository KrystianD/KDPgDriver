using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver.Builder
{
  public class FieldListBuilder<TModel>
  {
    public List<Expression<Func<TModel, object>>> Fields { get; } = new List<Expression<Func<TModel, object>>>();

    public FieldListBuilder<TModel> AddField(Expression<Func<TModel, object>> field)
    {
      Fields.Add(field);
      return this;
    }
  }
}