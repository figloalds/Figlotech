using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Diagnostics;
using Figlotech.Core.Helpers;
using Figlotech.Core.I18n;
using Figlotech.Core.Interfaces;

namespace Figlotech.Core {
    /// <summary>
    /// This is an utilitarian class, it provides quick static logic 
    /// for the rest of Figlotech Tools to operate
    /// </summary>
    public class Fi {

        public static Fi Tech = new Fi();
    }
}