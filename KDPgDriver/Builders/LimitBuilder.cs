using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using KDPgDriver.Utils;

namespace KDPgDriver.Builder
{
  // public interface ILimitBuilder
  // {
  //   RawQuery GetRawQuery();
  // }

  public class LimitBuilder 
  {
    internal int? LimitValue { get; set; }
    internal int? OffsetValue { get; set; }

    public LimitBuilder() { }

    public LimitBuilder Limit(int value)
    {
      LimitValue = value;
      return this;
    }

    public LimitBuilder Offset(int value)
    {
      OffsetValue = value;
      return this;
    }
  }
}