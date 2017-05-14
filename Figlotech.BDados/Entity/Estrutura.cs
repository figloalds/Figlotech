


using Figlotech.BDados.Attributes;
/**
* Figlotech.BDados.Entity.Structure
* Deprecated
* Was used by RepositoryValueObject to return an object-like ValueObject definition.
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.BDados.Entity;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity
{
    public class Structure
    {
        public static ForeignKeyDefinition[] ObterForeignKeys(Type t)
        {
            List<ForeignKeyDefinition> ForeignKeys = new List<ForeignKeyDefinition>();
            // Tabela: A tabela que tem a chave estrangeira.
            // Coluna: A coluna que referencia uma foreignkey
            // TabelaRef: A tabela referenciada pela foreignkey
            // ChaveRef: A coluna referenciada pela foreignkey
            //                              Na Tabela,          A coluna            Referencia tabela,      Coluna
            //ForeignKeys.Add(new BDForeignKey("caixa", "cx_terminal", "terminal", "teId"));
            //ForeignKeys.Add(new BDForeignKey("comanda", "cm_expediente", "expediente", "exId"));
            //ForeignKeys.Add(new BDForeignKey("comanda", "cm_cliente", "cliente", "clId"));
            //ForeignKeys.Add(new BDForeignKey("produto", "pr_categoria", "categoria_produto", "ctId"));
            //ForeignKeys.Add(new BDForeignKey("log_cadastro", "lc_usuario", "staff", "stId"));
            foreach(Type c in t.GetNestedTypes()) {
                foreach (FieldInfo f in c.GetFields())
                {
                    var v = (from a in f.CustomAttributes where a.AttributeType == typeof(ForeignKeyAttribute) select a);
                    if (v.Count() > 0)
                    {
                        ForeignKeyDefinition bdf = new ForeignKeyDefinition(
                                c.Name, f.Name,
                                (String)v.First().ConstructorArguments[0].Value,
                                (String)v.First().ConstructorArguments[1].Value
                            );
                    }
                }
            }
            return ForeignKeys.ToArray();
        }


        public static Boolean ValDt2(String DataType)
        {
            switch (DataType.ToUpper()) {
                case "BIGINT":
                case "BIT":
                case "DECIMAL":
                case "INT":
                case "MONEY":
                case "NUMERIC":
                case "SMALLMONEY":
                case "SMALLINT":
                case "TINYINT":
                case "REAL":
                case "DATE":
                case "DATETIME":
                case "DATETIME2":
                case "DATETIMEOFFSET":
                case "SMALLDATETIME":
                case "TIME":
                case "TIMESTAMP":
                case "UNIQUEIDENTIFIER":
                case "HIERARCHYID":
                case "XML":
                case "TEXT":
                case "NCHAR":
                case "NVARCHAR":
                case "VARCHAR":
                case "BINARY":
                case "VARBINARY":
                case "IMAGE":
                case "CURSOR":
                case "SQL_VARIANT":
                case "TABLE":
                    return true;
                default:
                    return false;
            }
        }

        private static String ValidarDataType(String DataType)
        {
            Match m = Regex.Match(DataType, @"(?<Type>[\w]*)\((?<Size>[\d,]*)\)");
            if(m.Success) {
                if (ValDt2(m.Groups["Type"].Value))
                    return DataType;
            }
            else {
                if (ValDt2(DataType))
                    return DataType;
            }
            #if DEBUG
            Console.WriteLine("ValidarDataType: {0} não é suportado pelo BDados, definindo como VARCHAR(20)", DataType); 
            #endif
            return "VARCHAR(128)"; // Pocus probabili
        }

        //public static RepositoryValueObject[] ObterTabelas(Type t)
        //{
        //    Type[] Classes = t.GetNestedTypes();
        //    List<RepositoryValueObject> Tabelas = new List<RepositoryValueObject>();
        //    foreach (Type classe in Classes)
        //    {
        //        // Cada classe aqui é uma tabela pro DB.
        //        // A classe aqui nao pode ser abstrata.
        //        try
        //        {
        //            FieldInfo[] NomesColunas = classe.GetFields();
        //            List<Coluna> ColunasAdd = new List<Coluna>();
        //            List<ChaveEstrangeira> FKeysAdd = new List<ChaveEstrangeira>();
        //             RepositoryValueObject Tab = (RepositoryValueObject)classe.GetConstructor(new Type[0]).Invoke(new Object[0]);
        //            foreach (FieldInfo Campo in NomesColunas)
        //            {
        //                String Tipo = "";
        //                String Opcoes = "";
        //                String NomeTipo = Campo.FieldType.Name;
        //                foreach (CustomAttributeData c in Campo.CustomAttributes)
        //                {
        //                    if (c.AttributeType == typeof(ColunaAttribute))
        //                    {
        //                        Tipo = ValidarDataType((String)c.ConstructorArguments[0].Value);
        //                        Opcoes = (String)c.ConstructorArguments[1].Value;
        //                        ColunasAdd.Add(new Coluna(Campo.Name, Tipo, Opcoes));
        //                        if (c.ConstructorArguments.Count > 2) {
        //                            if (((bool)c.ConstructorArguments[2].Value))
        //                                Tab.DefinirChavePrimaria(Campo.Name);
        //                        }
        //                    }
        //                    if (c.AttributeType == typeof(ForeignKeyAttribute))
        //                    {
        //                        String Tabela = (String)c.ConstructorArguments[0].Value;
        //                        String Coluna = (String)c.ConstructorArguments[1].Value;
        //                        FKeysAdd.Add(new ChaveEstrangeira(Entity.RepositoryValueObject.DBPrefixo+ classe.Name, Campo.Name, Tabela, Coluna));
        //                    }
        //                }
        //            }
        //            if (ColunasAdd.Count > 0) {
        //                Tab.Colunas.AddRange(ColunasAdd.ToArray());
        //                Tabelas.Add(Tab);
        //            }
        //        }
        //        catch (Exception x) {
        //            Debug.WriteLine(x.Message);
        //        }
        //    }
        //    return Tabelas.ToArray();
        //}
    }
}