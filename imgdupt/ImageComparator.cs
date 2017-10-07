using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.IO;
using System.Collections.Concurrent;
using System.Drawing;
using System.Drawing.Imaging;
using Figlotech.BDados;
using System.Security.Cryptography;
using System.Runtime.Serialization.Formatters.Binary;
using Figlotech.Core;

namespace imgdupt {
    class ImageComparator {
        String RandomsDir;
        String DuplicatesDir;
        String UniquesDir;
        static String DebugsDir;

        bool UniquesAreInitialized = false;
        List<Thread> UnicityCheckers = new List<Thread>();
        ConcurrentBag<ImageData> UniquesData = new ConcurrentBag<ImageData>();
        public int Tollerance = 2;
        public double MinDiff = 35;
        public bool DebugComparator = false;
        public int ComparatorSize = 32;
        static int DebugCount = 0;

        public ImageComparator(String Path) {
            if (!Path.EndsWith(@"\"))
                Path += @"\";
            RandomsDir = Path;
            DuplicatesDir = Path + @"Duplicates\";
            UniquesDir = Path + @"Uniques\";
            DebugsDir = Path + @"Debugs\";
            if (!Directory.Exists(DuplicatesDir))
                Directory.CreateDirectory(DuplicatesDir);
            if (!Directory.Exists(UniquesDir))
                Directory.CreateDirectory(UniquesDir);
        }

        #region **** Structs ****
        struct ImageData {
            public String Path;
            public Byte[] Data;
            public Int32 Size;
        }
        #endregion

        Byte[] GetImageData(Bitmap InputImage) {
            Byte[] ImageData = new Byte[ComparatorSize * ComparatorSize * 3];
            int RedOffset = 0;
            int GreenOffset = ComparatorSize * ComparatorSize;
            int BlueOffset = ComparatorSize * ComparatorSize * 2;
            int CurrentByte = 0;

            String fileName ="";


            using (Bitmap Image32 = new Bitmap(InputImage, ComparatorSize, ComparatorSize)) {
                for (int x = 0; x < Image32.Width; x++) {
                    for (int y = 0; y < Image32.Height; y++) {
                        Color Color1 = Image32.GetPixel(x, y);
                        ImageData[RedOffset + CurrentByte] = Color1.R;
                        ImageData[GreenOffset + CurrentByte] = Color1.G;
                        ImageData[BlueOffset + CurrentByte] = Color1.B;
                        CurrentByte++;
                    }
                }

                return ImageData;
            }

        }
        static int GetNumericDifference(int a, int b) {
            return a > b ? a - b : b - a;
        }

        double CheckDifference(Byte[] Value1, Byte[] Value2, Int32 Tollerance) {
            int Difference = 0;
            int RedOffset = 0;
            int GreenOffset = ComparatorSize * ComparatorSize;
            int BlueOffset = ComparatorSize * ComparatorSize * 2;
            for (int j = 0; j < ComparatorSize * ComparatorSize; j++) {
                Int32 MediaR = GetNumericDifference(Value1[j + RedOffset], Value2[j + RedOffset]);
                Int32 MediaG = GetNumericDifference(Value1[j + GreenOffset], Value2[j + GreenOffset]);
                Int32 MediaB = GetNumericDifference(Value1[j + BlueOffset], Value2[j + BlueOffset]);
                if ((MediaR + MediaG + MediaB) > Tollerance) {
                    Difference++;
                }

            }
            double result = Difference * 100 / (Value1.Length / 3);
            if (DebugComparator) {
                if (!Directory.Exists(DebugsDir))
                    Directory.CreateDirectory(DebugsDir);
                Int32 ComparatorFullSize = ComparatorSize * ComparatorSize;
                String Decision = "";
                if (result < MinDiff) {
                    Decision = "EqualImages";
                }
                else {
                    Decision = "DifferentImages";
                }
                using (Bitmap DifferenceMap = new Bitmap(ComparatorSize, ComparatorSize)) {
                    Int32 ComparatorSpot = 0;
                    for (int x = 0; x < ComparatorSize; x++) {
                        for (int y = 0; y < ComparatorSize; y++) {
                            int r1, g1, b1;
                            r1 = Value1[ComparatorSpot];
                            g1 = Value1[ComparatorSpot + ComparatorFullSize];
                            b1 = Value1[ComparatorSpot + (ComparatorFullSize * 2)];
                            int r2, g2, b2;
                            r2 = Value2[ComparatorSpot];
                            g2 = Value2[ComparatorSpot + ComparatorFullSize];
                            b2 = Value2[ComparatorSpot + (ComparatorFullSize * 2)];
                            int r3 = r1 > r2 ? r1 - r2 : r2 - r1;
                            int g3 = g1 > g2 ? g1 - g2 : g2 - g1;
                            int b3 = b1 > b2 ? b1 - b2 : b2 - b1;
                            DifferenceMap.SetPixel(x, y, Color.FromArgb(r3, g3, b3));
                            ComparatorSpot++;
                        }
                    }
                    DifferenceMap.Save(DebugsDir + String.Format(Decision + @"_{0}.png", DebugCount++), ImageFormat.Png);
                }
            }
            return result;
        }
        public void Initialize() {
            if (UniquesAreInitialized) return;
            //GetUniquesData();
            UniquesAreInitialized = true;
        }
        void GetUniquesData() {
            Fi.Tech.WriteLine("Iniciando separador de imagens...");
            String[] Uniques = Directory.GetFiles(UniquesDir);
            foreach (var UniqueFile in Uniques) {
                using (FileStream fs = new FileStream(UniqueFile, FileMode.Open)) {
                    Bitmap Unique = new Bitmap(fs);
                    ImageData Data = new ImageData() {
                        Path = UniqueFile,
                        Data = GetImageData(Unique),
                        Size = Unique.Width * Unique.Height
                    };
                    UniquesData.Add(Data);
                    Unique.Dispose();
                }
            };
            Fi.Tech.WriteLine("Separador inicializado.");
        }
        public void RunDuplicateTester() {
            List<String> ImagesToCheck = new List<String>();
            ImagesToCheck.AddRange(Directory.GetFiles(RandomsDir));
            if (ImagesToCheck.Count == 0) return;

            Initialize();
            while (ImagesToCheck.Count > 0) {
                int litc = ImagesToCheck.Count - 1;
                String ImagemAtual = ImagesToCheck[litc];
                try {
                    bool IsUnique = true;
                    Byte[] CurrentImageData;
                    Int32 Size;
                    ImageData FoundUnique = new ImageData();
                    Int32 FoundUniqueIndex;
                    using (var fs = File.Open(ImagesToCheck[litc], FileMode.Open)) {
                        using (Image Random1 = Bitmap.FromStream(fs)) {
                            var Random = (Bitmap)Random1;
                            CurrentImageData = GetImageData(Random);
                            Size = Random.Width * Random.Height;
                            foreach (ImageData Unique in UniquesData) {
                                double Difference = CheckDifference(CurrentImageData, Unique.Data, Tollerance);
                                if (Difference < MinDiff) {
                                    IsUnique = false;
                                    FoundUnique = Unique;
                                    Fi.Tech.WriteLine($"Repetida {Path.GetFileName(ImagemAtual)}");
                                    ImagesToCheck.Remove(ImagemAtual);
                                    break;
                                }
                            }
                        }
                    }
                    if (IsUnique) {
                        ImageData ThisUnique = new ImageData() {
                            Path = UniquesDir + Path.GetFileName(ImagemAtual)
                        };
                        File.Move(ImagemAtual, UniquesDir + Path.GetFileName(ImagemAtual));
                        ThisUnique.Size = Size;
                        ThisUnique.Data = CurrentImageData;
                        UniquesData.Add(ThisUnique);
                        ImagesToCheck.Remove(ImagemAtual);
                    }
                    else {
                        if (Size > FoundUnique.Size) {
                            lock ("BiggerThanUnique") {
                                if (Size > FoundUnique.Size) {
                                    FoundUnique.Path = UniquesDir + Path.GetFileName(ImagemAtual);
                                    File.Move(ImagemAtual, UniquesDir + Path.GetFileName(ImagemAtual));
                                    FoundUnique.Data = CurrentImageData;
                                    FoundUnique.Size = Size;
                                    try {
                                        File.Move(FoundUnique.Path, DuplicatesDir + Path.GetFileName(FoundUnique.Path));
                                    }
                                    catch (Exception) { }
                                    ImagesToCheck.Remove(ImagemAtual);
                                }
                            }
                        }
                        else {
                            try {
                                File.Move(ImagemAtual, DuplicatesDir + Path.GetFileName(ImagemAtual));
                            }
                            catch (Exception) {
                                File.Delete(ImagemAtual);
                            }
                            ImagesToCheck.Remove(ImagemAtual);
                        }
                    }
                }
                catch (Exception e) {
                    ImagesToCheck.RemoveAt(litc);
                }
            }
        }

        public void DupTestInMem() {
            List<String> ImagesToCheck = new List<String>();
            List<String> UniquesFiles = new List<String>();
            List<String> DuplicatesFiles = new List<String>();
            ImagesToCheck.AddRange(Directory.GetFiles(RandomsDir, "*", SearchOption.TopDirectoryOnly));
            if (ImagesToCheck.Count == 0) return;

            if (!UniquesAreInitialized) {
                Initialize();
            }
            int TotalImgs = ImagesToCheck.Count;
            while (ImagesToCheck.Count > 0) {
                String ImagemAtual = ImagesToCheck[0];
                try {
                    bool IsUnique = true;
                    Byte[] RandomValue;
                    Int32 Size;
                    ImageData FoundUnique = new ImageData();
                    Int32 FoundUniqueIndex;
                    using (Bitmap Random = (Bitmap)Bitmap.FromFile(ImagemAtual)) {
                        RandomValue = GetImageData(Random);
                        Size = Random.Width * Random.Height;
                        Parallel.ForEach(UniquesData, Unique => {
                            if (!IsUnique)
                                return;
                            double Difference = CheckDifference(RandomValue, Unique.Data, Tollerance);
                            if (Difference < MinDiff) {
                                IsUnique = false;
                                FoundUnique = Unique;
                                Fi.Tech.WriteLine($"Repetida {Path.GetFileName(ImagemAtual)}");
                                return;
                            }
                        });
                    }
                    if (IsUnique) {
                        ImageData ThisUnique = new ImageData() {
                            Path = UniquesDir + Path.GetFileName(ImagemAtual)
                        };
                        //File.Move(ImagemAtual, UniquesDir + Path.GetFileName(ImagemAtual));
                        UniquesFiles.Add(ImagemAtual);
                        ThisUnique.Size = Size;
                        ThisUnique.Data = RandomValue;
                        UniquesData.Add(ThisUnique);
                        ImagesToCheck.Remove(ImagemAtual);
                    }
                    else {
                        if (Size > FoundUnique.Size) {
                            if (Size > FoundUnique.Size) {
                                FoundUnique.Path = ImagemAtual;
                                //File.Move(ImagemAtual, UniquesDir + Path.GetFileName(ImagemAtual));
                                UniquesFiles.Add(ImagemAtual);
                                FoundUnique.Data = RandomValue;
                                FoundUnique.Size = Size;
                                try {
                                    //File.Move(FoundUnique.Path, DuplicatesDir + Path.GetFileName(FoundUnique.Path));
                                }
                                catch (Exception) { }
                                UniquesFiles.Remove(FoundUnique.Path);
                                DuplicatesFiles.Add(FoundUnique.Path);
                                ImagesToCheck.Remove(ImagemAtual);
                            }
                        }
                        else {
                            DuplicatesFiles.Add(ImagemAtual);
                            ImagesToCheck.Remove(ImagemAtual);
                        }
                    }
                }
                catch (Exception e) {
                    Fi.Tech.WriteLine(e.Message);
                }
            }
            foreach (String Arq in DuplicatesFiles) {
                File.Move(Arq, Path.Combine(DuplicatesDir + Path.GetFileName(Arq)));
            }
            foreach (String Arq in UniquesFiles) {
                File.Move(Arq, Path.Combine(UniquesDir + Path.GetFileName(Arq)));
            }
        }

    }
}
