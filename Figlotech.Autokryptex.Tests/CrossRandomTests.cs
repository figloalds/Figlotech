using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Text;
using Figlotech.Autokryptex.EncryptMethods;
using System.Drawing;

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

        [TestMethod]
        public void CrossRandomShouldMakeABeautifulImg() {
            Bitmap bmp = new Bitmap(500,500);
            CrossRandom cr = new CrossRandom(12345678);
            CrossRandom.UseAppSecret("Using an App secret for a change can help");

            cr.UseInstanceSecret("ASDFASDF");
            for (int x = 0; x < bmp.Width; x++) {
                for (int y = 0; y< bmp.Width; y++) {
                    int r = cr.Next(256);
                    int g = cr.Next(256);
                    int b = cr.Next(256);
                    bmp.SetPixel(x, y, Color.FromArgb(r, g, b));
                }
            }

            bmp.Save("CRYPTRANDOM.bmp");

            // I don't think its possible to assert anything here.
            // The human necessary validation is: 
            // The image MUST be very noisy.
            // And it CANNOT CONTAIN ANY PATTERN.
            // Because patterns in the image are indicators period,
            // and our pseudo-random algorithm can't have a period.

        }
    }
}
