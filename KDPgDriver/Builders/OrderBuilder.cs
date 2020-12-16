using System;
using System.Linq.Expressions;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public interface IOrderBuilder
  {
    RawQuery GetRawQuery();
  }

  public class OrderBuilder<TModel> : IOrderBuilder
  {
    private RawQuery _rq = new RawQuery();

    public OrderBuilder<TModel> OrderBy(Expression<Func<TModel, object>> exp)
    {
      var e = NodeVisitor.VisitFuncExpression(exp);

      _rq.AppendSeparatorIfNotEmpty();
      _rq.Append(e.RawQuery);

      return this;
    }

    public OrderBuilder<TModel> OrderByDescending(Expression<Func<TModel, object>> exp)
    {
      var e = NodeVisitor.VisitFuncExpression(exp);

      _rq.AppendSeparatorIfNotEmpty();
      _rq.Append(e.RawQuery);
      _rq.Append(" DESC");

      return this;
    }

    public RawQuery GetRawQuery() => _rq;
  }
}