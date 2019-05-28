using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex.EncryptMethods
{
    public class AutokryptexEncryptor : IEncryptionMethod
    {
        AggregateEncryptor encryptor = new AggregateEncryptor();

        String instancePassword;

        public AutokryptexEncryptor(String password, int MaxEncryptors = 6) {
            instancePassword = password;
            CrossRandom cr = new CrossRandom(Int32.MaxValue ^ 123456789);
            var passwordBytes = MathUtils.CramString(password, 16);
            int[] args = new int[password.Length];
            for(int i = 0; i < args.Length; i++) {
                args[i] = password[i] ^ cr.Next(77777);
            }
            Init(args, MaxEncryptors);
        }
        
        public byte[] Decrypt(byte[] en) {
            return encryptor.Decrypt(en);
        }

        public byte[] Encrypt(byte[] en) {
            return encryptor.Encrypt(en);
        }

        private void Init(int[] args, int maxEncryptors) {
            if (args.Length < 2) {
                throw new Exception("Crazy Locking Engine requires 2 or more integers as 'Key', preferably big primes");
            }
            var passBytes = Fi.StandardEncoding.GetBytes(instancePassword);
            CrossRandom cr = new CrossRandom(Int32.MaxValue ^ args[0]);
            Type[] availableMethods = new Type[] {
                typeof(BinaryBlur),
                typeof(BinaryNegativation),
                typeof(BinaryScramble),
                typeof(EnigmaEncryptor),
            };

            foreach(var a in args) {
                var em = availableMethods[cr.Next(availableMethods.Length)];
                var ctors = em.GetConstructors();
                if(ctors.Any(
                    c=> c.GetParameters().Length == 1 && 
                    c.GetParameters()[0].ParameterType== typeof(int)
                )) {
                    encryptor.Add(
                        (IEncryptionMethod)
                        Activator.CreateInstance(em,
                            new Object[] { a ^ cr.Next(Int32.MaxValue) }
                        )
                    );
                } else 
                if(ctors.Any(
                    c => c.GetParameters().Length == 0 
                )) {
                    encryptor.Add(
                        (IEncryptionMethod)
                        Activator.CreateInstance(em)
                    );
                }
            }

            while(encryptor.Count > maxEncryptors) {
                encryptor.RemoveAt(cr.Next(encryptor.Count));
            }

            encryptor.Add(new AesEncryptor(instancePassword));
        }

    }
}
