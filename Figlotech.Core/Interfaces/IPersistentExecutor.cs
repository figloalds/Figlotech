﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Interfaces
{
    public interface IContinuousExecutor
    {
        void Start();
        void Stop(bool wait);
    }
}
