
/**
 * These classes are unnused (or should be)
 * They were an early attempt into Attribute Validation.
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {
    public class Intervalo {
        public int Minimo;
        public int Maximo;
        public Intervalo(int minimo, int maximo) {
            Minimo = minimo;
            Maximo = maximo;
        }
    }
    public class CampoAttribute : Attribute {
        public Intervalo Tamanho = null;
        public bool Obrigatorio = false;
        public String Mascara = null;
        
        public CampoAttribute() { }
    }
}
