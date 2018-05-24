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
      string q = $"\nUPDATE \"{driver.Schema}\".\"{Builder.TableName}\"\n";

      string setPart = string.Join(", ", updateParts.Select(x => $"{x.Key} = {x.Value}"));
      q += $"SET {setPart}\n";

      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $"WHERE {wherePart}";

      return q + "\n";
    }
  }
}