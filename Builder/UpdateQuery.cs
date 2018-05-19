using System.Collections.Generic;
using System.Linq;

namespace KDPgDriver.Builder
{
  public class UpdateQuery<TOut>
  {
    public ParametersContainer Parameters { get; }
    public IBaseQueryBuilder Builder { get; }

    public Dictionary<string, string> updateParts = new Dictionary<string, string>();

    public UpdateQuery(IBaseQueryBuilder baseQueryBuilder, ParametersContainer parameters)
    {
      Parameters = parameters;
      Builder = baseQueryBuilder;
    }

    public string GetQuery()
    {
      string q = $"UPDATE \"{Builder.Driver.Schema}\".\"{Builder.TableName}\"";

      string setPart = string.Join(", ", updateParts.Select(x => $"{x.Key} = {x.Value}"));
      q += $" SET {setPart}";

      string wherePart = Builder.GetWherePart();
      if (wherePart.Length > 0)
        q += $" WHERE {wherePart}";

      return q;
    }
  }
}