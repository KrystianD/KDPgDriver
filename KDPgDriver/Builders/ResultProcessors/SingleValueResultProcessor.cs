﻿using KDPgDriver.Utils;

namespace KDPgDriver.Builders
{
  public class SingleValueResultProcessor : IResultProcessor
  {
    private readonly KDPgValueType _type;

    public int FieldsCount => 1;

    public SingleValueResultProcessor(KDPgValueType type)
    {
      _type = type;
    }

    public object ParseResult(object[] values)
    {
      return Helper.ConvertFromRawSqlValue(_type, values[0]);
    }
  }
}