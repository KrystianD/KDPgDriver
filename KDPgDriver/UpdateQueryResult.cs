using System.Collections;
using System.ComponentModel.DataAnnotations;
using System.IO;

namespace KDPgDriver
{
  public class UpdateQueryResult
  {
    
  }
  public class InsertQueryResult
  {
    public int LastInsertId { get; }

    public InsertQueryResult(int lastInsertId)
    {
      LastInsertId = lastInsertId;
    }
  }
}