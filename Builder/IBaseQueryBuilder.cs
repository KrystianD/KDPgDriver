using System.Collections.Generic;
using System.Linq.Expressions;

namespace KDPgDriver.Builder {
  public interface IBaseQueryBuilder
  {
    Driver Driver { get; }
    string TableName { get; }

    string GetWherePart();
  }
}