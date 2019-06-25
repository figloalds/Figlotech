using Figlotech.Core;
using Figlotech.Core.Autokryptex.EncryptionMethods;
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

        byte[] instancePassword;
        String encodedPassword;
        TwoWayRsaEncryptor TwoWayEncryptor;
        CrossRandom cr;

        public bool MultiPassRandomEncoder { get; set; } = false;

        public AutokryptexEncryptor(String password,int MaxEncryptors = 6, int seed = 179426447) {
            cr = new CrossRandom(Int32.MaxValue ^ seed);
            instancePassword = cr.CramString(password, 256);
            int[] args = new int[256];
            for(int i = 0; i < args.Length; i++) {
                args[i] = instancePassword[i] ^ cr.Next(Int32.MaxValue);
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
            var passBytes = instancePassword;
            Type[] availableMethods = new Type[] {
                typeof(BinaryBlur),
                typeof(BinaryNegativation),
                typeof(BinaryScramble),
                typeof(EnigmaEncryptor),
            };
            var pickedKeys = new Queue<int>();
            for(int i = 0; i < maxEncryptors; i++) {
                pickedKeys.Enqueue(args[cr.Next(args.Length - 1)]);
            }

            foreach(var a in pickedKeys) {
                var em = availableMethods[cr.Next(availableMethods.Length)];
                var ctors = em.GetConstructors();
                if(ctors.Any(
                    c=> c.GetParameters().Length == 1 && 
                    c.GetParameters()[0].ParameterType == typeof(int)
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
                    var inst =
                        (IEncryptionMethod)
                        Activator.CreateInstance(em);

                    passBytes = inst.Encrypt(passBytes);

                    if(MultiPassRandomEncoder && inst.GetType() != typeof(EnigmaEncryptor)) {
                        encryptor.Add(
                            inst
                        );
                    }
                }
            }

            while(encryptor.Count > maxEncryptors) {
                encryptor.RemoveAt(cr.Next(encryptor.Count));
            }
            encodedPassword = Convert.ToBase64String(passBytes);
            encryptor.Add(new AesEncryptor(Fi.StandardEncoding.GetString(instancePassword)));
        }

    }
}
