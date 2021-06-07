using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using KDPgDriver.Traverser;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public class FieldListBuilder<TModel>
  {
    private readonly HashSet<KdPgColumnDescriptor> _fieldsSet = new HashSet<KdPgColumnDescriptor>();

    public List<KdPgColumnDescriptor> Fields { get; } = new List<KdPgColumnDescriptor>();

    public FieldListBuilder<TModel> AddField(Expression<Func<TModel, object>> field)
    {
      var column = NodeVisitor.EvaluateExpressionToColumn(field);

      if (_fieldsSet.Add(column))
        Fields.Add(column);

      return this;
    }
  }
}