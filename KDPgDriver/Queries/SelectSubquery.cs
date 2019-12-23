using KDPgDriver.Builders;
using KDPgDriver.Utils;

namespace KDPgDriver.Queries
{
  public interface ISelectSubquery : IQuery
  {
  }

  public class SelectSubquery<TValue> : ISelectSubquery
  {
    private readonly ISelectQuery _selectQuery;

    public SelectSubquery(ISelectQuery selectQuery)
    {
      _selectQuery = selectQuery;
    }

    public RawQuery GetRawQuery(string defaultSchema = null)
    {
      return _selectQuery.GetRawQuery(defaultSchema);
    }
  }
}