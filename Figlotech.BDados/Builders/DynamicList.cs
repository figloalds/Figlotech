/**
 * Figlotech.BDados.Builders.DynamicList
 * This class is used as a quick proxy to qualify JoinBuilder.BuildObject return value.
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Builders {
    public class DynamicList : List<dynamic> {
        public IList<T> Qualify<T>() {
            try {
                var json = JsonConvert.SerializeObject(this);
                var obj = JsonConvert.DeserializeObject<List<T>>(json);
                return obj;
            } catch (Exception x) {
                throw new Exception($"Dynamic list could not be qualified to given type: {typeof(T).Name}", x);
            }
        }
    }
}
