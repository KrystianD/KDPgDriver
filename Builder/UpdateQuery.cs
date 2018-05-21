using System.Collections.Generic;
using System.Linq;

namespace KDPgDriver.Builder
{
  public class UpdateQuery<TOut>
  {
    public ParametersContainer Parameters { get; }
    public IQueryBuilder Builder { get; }

    public Dictionary<string, string> updateParts = new Dictionary<string, string>();

    public UpdateQuery(IQueryBuilder queryBuilder, ParametersContainer parameters)
    {
      Parameters = parameters;
      Builder = queryBuilder;
    }

    public string GetQuery(Driver driver)
    {
      string q = $"UPDATE \"{driver.Schema}\".\"{Builder.TableName}\"";

      string setPart = string.Join(", ", updateParts.Select(x => $"{x.Key} = {x.Value}"));
      q += $" SET {setPart}";

      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }
  }
}