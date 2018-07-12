namespace KDPgDriver.Results {
  public class InsertQueryResult
  {
    public int? LastInsertId { get; }

    public InsertQueryResult(int? lastInsertId)
    {
      LastInsertId = lastInsertId;
    }
  }
}