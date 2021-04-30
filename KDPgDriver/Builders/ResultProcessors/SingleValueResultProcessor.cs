using KDPgDriver.Types;
using KDPgDriver.Utils;

namespace KDPgDriver.Builders.ResultProcessors
{
  public class SingleValueSelectResultProcessor : ISelectResultProcessor
  {
    private readonly KDPgValueType _type;

    public int FieldsCount => 1;

    public SingleValueSelectResultProcessor(KDPgValueType type)
    {
      _type = type;
    }

    public object ParseResult(object[] values)
    {
      return PgTypesConverter.ConvertFromRawSqlValue(_type, values[0]);
    }
  }
}