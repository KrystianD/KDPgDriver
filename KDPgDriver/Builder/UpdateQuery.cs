using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public interface IUpdateQuery : IQuery { }

  public class UpdateQuery<TOut> : IUpdateQuery
  {
    private readonly UpdateStatementsBuilder<TOut> _updateStatementsBuilder;
    private readonly IQueryBuilder _builder;

    public UpdateQuery(IQueryBuilder queryBuilder, UpdateStatementsBuilder<TOut> updateStatementsBuilder)
    {
      _updateStatementsBuilder = updateStatementsBuilder;
      _builder = queryBuilder;
    }

    public RawQuery GetQuery(Driver driver)
    {
      string schema = _builder.SchemaName ?? driver.Schema;

      RawQuery rq = new RawQuery();
      rq.Append("UPDATE ", Helper.QuoteTable(_builder.TableName, schema), "\n");

      rq.Append("SET ");
      bool first = true;
      foreach (var (name, rawQuery) in _updateStatementsBuilder.UpdateParts) {
        if (!first)
          rq.Append(", ");

        rq.Append(Helper.Quote(name), " = ");
        rq.Append(rawQuery);

        first = false;
      }

      // string q = $"SELECT {selectStr} FROM \"{schema}\".\"{Builder.TableName}\"";
      RawQuery wherePart = _builder.GetWherePart();
      if (!wherePart.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(wherePart);
      }

      return rq;
    }
  }
}