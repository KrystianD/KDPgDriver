﻿using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using KDLib;
using KDPgDriver.Types;

namespace KDPgDriver.Utils
{
  public class PgValue
  {
    public object Value { get; }
    public KDPgValueType Type { get; }

    public PgValue(object value, KDPgValueType type)
    {
      Value = value;
      Type = type;
    }

    // ReSharper disable once HeapView.ObjectAllocation.Evident
    public static readonly PgValue Null = new PgValue(null, KDPgValueTypeInstances.Null);
  }
}