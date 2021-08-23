using System;
using System.Collections.Generic;

namespace KDPgDriver.Results
{
  public class InsertQueryResult
  {
    public readonly List<int> LastInsertIds;

    // Compatibility API
    public bool RowInserted => LastInsertIds.Count switch {
        0 => false,
        1 => true,
        _ => throw new Exception("Multiple rows returned"),
    };

    public int? LastInsertId => LastInsertIds.Count switch {
        0 => null,
        1 => LastInsertIds[0],
        _ => throw new Exception("Multiple rows returned"),
    };

    internal InsertQueryResult(List<int> lastInsertIds)
    {
      LastInsertIds = lastInsertIds;
    }
  }
}