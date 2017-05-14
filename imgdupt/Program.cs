using System;
using System.IO;

namespace imgdupt
{
    class Program
    {
        static void Main(string[] args)
        {
            if(args.Length != 1) {
                Console.WriteLine("Usage: imgdupt <directory_path>");
                return;
            }
            if(!Directory.Exists(args[0])) {
                Console.WriteLine($"Directory does not exist {args[0]}");
                return;
            }

            ImageComparator comparator = new ImageComparator(args[0]);
            comparator.RunDuplicateTester();
        }
    }
}