using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public interface IDeleteQuery : IQuery { }

  public class DeleteQuery<TOut> : IDeleteQuery
  {
    private readonly IQueryBuilder _builder;

    public DeleteQuery(IQueryBuilder queryBuilder)
    {
      _builder = queryBuilder;
    }

    public RawQuery GetQuery(Driver driver)
    {
      string schema = _builder.SchemaName ?? driver.Schema;

      RawQuery rq = new RawQuery();
      rq.Append("DELETE FROM ", Helper.QuoteTable(_builder.TableName, schema));

      RawQuery wherePart = _builder.GetWherePart();
      if (!wherePart.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(wherePart);
      }

      return rq;
    }
  }
}