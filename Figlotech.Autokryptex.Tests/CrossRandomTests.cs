using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using Figlotech.Autokryptex.EncryptMethods;

namespace Figlotech.Autokryptex.Tests
{
    [TestClass]
    public class CrossRandomTests {
        [TestMethod]
        public void CrossRandomShouldBeReproducible() {
            int sz = 128;
            int[] listA = new int[sz];
            int[] listB = new int[sz];

            CrossRandom crA = new CrossRandom(12345678);
            CrossRandom crB = new CrossRandom(12345678);

            crA.UseInstanceSecret("What am I doing of my life...");
            crB.UseInstanceSecret("What am I doing of my life...");

            for (int i = 0; i < listA.Length; i++) {
                listA[i] = crA.Next(100);
            }
            for (int i = 0; i < listB.Length; i++) {
                listB[i] = crB.Next(100);
            }
            for(int i = 0; i < listA.Length; i++) {
                Assert.AreEqual(listA[i], listB[i]);
            }
        }
    }
}
