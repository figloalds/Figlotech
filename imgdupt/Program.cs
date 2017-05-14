using System;
using System.IO;

namespace imgdupt
{
    class Program
    {

        static void Usage() {
            Console.WriteLine("Usage: imgdupt <directory_path> <options>");
            Console.WriteLine("Options:");
            Console.WriteLine("\t-sz <size:int> :");
            Console.WriteLine("\t\tSets the precision, higher value means more accurate but slower comparison");
            Console.WriteLine("\t-mindiff <mindiff:int> :");
            Console.WriteLine("\t\tSets the minimum difference between images (in %) for the comparator to consider them different");
            Console.WriteLine("\t-tol <tolerance:int> :");
            Console.WriteLine("\t\tSets the pixel tolerance, low values may cause the comparator to see 2 apparently equal images as different because of differing artifact/quality");
            Console.WriteLine("");
            Console.WriteLine("The comparator is able to read any GDI+ compliant format and even compare images of different extensions, it will always give preference for larger images when finding duplicates.");
            return;
        }

        static void Main(string[] args) {
            Console.WriteLine("------------------------------------------------");
            Console.WriteLine("-- Figlotech Tools");
            Console.WriteLine("-- imgdupt");
            Console.WriteLine("-- Scans a folder and separates duplicate images");
            Console.WriteLine("------------------------------------------------");
            if (args.Length == 1) {
                Usage();
                return;
            }

            ImageComparator comparator = new ImageComparator(args[0]);
            for (int i = 0; i < args.Length; i++) {
                if (args[i] == "-sz") {
                    try {
                        int size = Convert.ToInt32(args[i + 1]);
                        comparator.ComparatorSize = size * 2;
                    } catch (Exception) {
                        Usage();
                        return;
                    }
                }
                if (args[i] == "-tol") {
                    try {
                        int size = Convert.ToInt32(args[i + 1]);
                        comparator.Tollerance = size * 2;
                    } catch (Exception) {
                        Usage();
                        return;
                    }
                }
            }
            if(!Directory.Exists(args[0])) {
                Console.WriteLine($"Directory does not exist {args[0]}");
                return;
            }

            comparator.RunDuplicateTester();
        }
    }
}