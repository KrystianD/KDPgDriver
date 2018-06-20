using System.Collections.Generic;
using System.Linq;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  public interface IUpdateQuery
  {
    RawQuery GetQuery(Driver driver);
  }

  public class UpdateQuery<TOut> : IUpdateQuery
  {
    // public ParametersContainer Parameters { get; }
    public IQueryBuilder Builder { get; }

    public Dictionary<string, RawQuery> updateParts = new Dictionary<string, RawQuery>();

    public UpdateQuery(IQueryBuilder queryBuilder /*, ParametersContainer parameters*/)
    {
      // Parameters = parameters;
      Builder = queryBuilder;
    }

    public RawQuery GetQuery(Driver driver)
    {
      string schema = Builder.SchemaName ?? driver.Schema;

      // string q = $"\nUPDATE \"{schema}\".\"{Builder.TableName}\"\n";
      //
      // string setPart = string.Join(", ", updateParts.Select(x => $"{x.Key} = {x.Value}"));
      // q += $"SET {setPart}\n";

      // string wherePart = Builder.GetWherePart();
      // if (wherePart.Length > 0)
      //   q += $"WHERE {wherePart}";

      // return q + "\n";


      RawQuery rq = new RawQuery();
      rq.Append("UPDATE ", Helper.QuoteTable(Builder.TableName, schema), "\n");

      rq.Append("SET ");
      bool first = true;
      foreach (var (name, rawQuery) in updateParts) {
        if (!first)
          rq.Append(", ");

        rq.Append(Helper.Quote(name), " = ");
        rq.Append(rawQuery);
        
        first = false;
      }

      // string q = $"SELECT {selectStr} FROM \"{schema}\".\"{Builder.TableName}\"";
      RawQuery wherePart = Builder.GetWherePart();
      if (!wherePart.IsEmpty) {
        rq.Append(" WHERE ");
        rq.Append(wherePart);
      }

      return rq;
    }
  }
}