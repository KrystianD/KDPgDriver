using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver.Builders {
  public interface IQueryBuilder
  {
    // Driver Driver { get; }
    string TableName { get; }
    string SchemaName { get; }

    IWhereBuilder GetWhereBuilder();
  }
}