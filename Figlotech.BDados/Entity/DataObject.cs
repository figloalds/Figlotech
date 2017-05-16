using Figlotech.BDados.Attributes;
using Figlotech.BDados.Builders;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Requirements;
using Figlotech.Core;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Management;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity
{
    public class DataObject<T> : BaseDataObject, IBusinessObject<T> where T : IDataObject, new() {

        [PrimaryKey]
        [Field(Options = "NOT NULL AUTO_INCREMENT PRIMARY KEY")]
        public override long Id { get; set; } = 0;

        [Field(AllowNull = true, DefaultValue = null)]
        public override DateTime? UpdatedTime { get; set; } = DateTime.UtcNow;

        [NoUpdate]
        [Field(Type = "TIMESTAMP", AllowNull = false, DefaultValue = "CURRENT_TIMESTAMP")]
        public override DateTime CreatedTime { get; set; } = DateTime.UtcNow;

        [ReliableId]
        [Field(Size = 64, AllowNull = false, Unique = true)]
        public override RID RID { get; set; }

        public override bool IsPersisted() {
            return Id > 0;
        }

        private static IntEx _cpuhash;
        private static IntEx CpuHash {
            get {
                if (_cpuhash != null) {
                    return _cpuhash;
                } else {
                    return _cpuhash = new IntEx(FTH.CpuId, IntEx.Hexadecimal);
                }
            }
        }

        private static int sequentia = 0;

        public List<IValidationRule<T>> ValidationRules { get; set; } = new List<IValidationRule<T>>();

        public DataObject() {

            StackTrace stackTrace = new StackTrace();           // get call stack
            StackFrame[] stackFrames = stackTrace.GetFrames();  // get method calls (frames)

            // write call stack method names
            foreach (StackFrame stackFrame in stackFrames) {
                if (
                    stackFrame.GetMethod().Name.Contains("DeserializeObject") ||
                    stackFrame.GetMethod().Name.Contains("LoadAll") ||
                    stackFrame.GetMethod().Name.Contains("LoadByRid") ||
                    stackFrame.GetMethod().Name.Contains("BuildObject") ||
                    stackFrame.GetMethod().Name.Contains("Qualify") ||
                    stackFrame.GetMethod().Name.Contains("Query")) {
                    return;
                }
            }
            Init();
        }

        public DataObject(bool NoInit) {
            if (!NoInit)
                Init();
        }

        public override ValidationErrors Validate() {
            ValidationErrors ve = new ValidationErrors();
            var myType = this.GetType();

            var myValues = new List<MemberInfo>();
            myValues.AddRange(this.GetType().GetFields());
            myValues.AddRange(this.GetType().GetProperties());

            foreach(var a in ValidationRules) {
                foreach(var err in a.Validate(this)) {
                    ve.Add(err);
                }
            }

            // Here goes LogicalField validation to initialize 
            // relevant fields for business logic and validation
            foreach (var field in myValues.Where((f) => f.GetCustomAttribute<FieldAttribute>() != null)) {
                var info = field.GetCustomAttribute<FieldAttribute>();
                if (!info.AllowNull && ReflectionTool.GetMemberValue(field, this) == null) {
                    ve.Add($"{field.Name}", $"{field.Name} cannot be null.");
                }
            }

            // Validations
            foreach (var field in myValues.Where((f) => f.GetCustomAttribute<ValidationAttribute>() != null)) {
                var vAttribute = field.GetCustomAttribute<ValidationAttribute>();
                if (!vAttribute.Validate(ReflectionTool.GetMemberValue(field, this))) {
                    ve.Add($"{field.Name}", vAttribute.GetValidationMessage(field));
                }
            }

            return ve;
        }

        private string FillBlanks(String rid) {
            var c = 64 - rid.Length;
            return FTH.GenerateIdString(rid, c) + rid;
        }

        public override void Init() {
            IntEx i = new IntEx((DateTime.Now.Ticks * 10000) + (sequentia++%10000));
            i *= 100000000;
            i *= r.Next(100000000);
            i *= (long) Math.Pow(FTH.CpuId.Length, 16);
            //i += cpuhash;
            RID = FillBlanks((String) i.ToString(IntEx.Base36));
            UpdatedTime = DateTime.UtcNow;
            CreatedTime = DateTime.UtcNow;
        }

    }
}
