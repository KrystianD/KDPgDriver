using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver.Builder {
  public interface IQueryBuilder
  {
    // Driver Driver { get; }
    string TableName { get; }
    string SchemaName { get; }

    RawQuery GetWherePart();
  }
}