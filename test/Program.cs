using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace test
{
    class Program
    {
        static void Main(string[] args) {
            var path = @"C:\Users\felyp\Desktop\PROJETOS\ErpSoftLeader\.dist\EslDesktop\bin";

            //Fi.Tech.StdoutLogs(true);

            if (!Directory.Exists(path)) {
                Console.WriteLine("Nao encontrou a pasta ESL2");
                Console.ReadKey();
                return;
            }

            int processed = 0, ignored = 0;

            var localFS = new FileAccessor(path);
            //var blobFS = new FileAccessor("C:\\ErpSoftLeader");
            //var blobFS = new BlobFileAccessor("127.0.0.1:10000", "devstoreaccount1", "testblobct", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
            var blobFS = new BlobFileAccessor("felyper", "VaKDbICKepMsYKYQ6i01B12AeBPmo4MCq7plnDlVrlLiFGAzwaJ7PAFNDmJUKzmktKdykT/bRVW+5x7PfDFKAQ==", "erpslv2");

            Benchmarker Bench = new Benchmarker("Upload Marker");
            var copy = new SmartCopy(localFS, new SmartCopyOptions() {
                UseGZip = true,
                UseHash = true,
                UseHashList = true,
                Recursive = true,
            });

            copy.SetRemote(blobFS);

            copy.Excludes.Add("cef_redist.zip");

            int went = 0, max = 0;
            copy.OnReportTotalFilesCount += (c) => max = c;

            copy.OnReportProcessedFile += (c, f) => {
                Bench.Mark(f);
                Console.WriteLine($"[{(c ? "SUBIU" : "IGNOROU")}] [{went}/{max}] {f}");
                if (c) {
                    processed++;
                }
                else {
                    ignored++;
                }
                went++;
            };

            copy.MirrorUp("");

            Bench.FinalMark();
            Console.WriteLine("UPLOAD OK");
            Console.WriteLine("INDEX OK");
            Console.WriteLine("CLEAR OK");
            Console.WriteLine("ALL DONE");

            Console.WriteLine($"Uploads {processed}");
            Console.WriteLine($"Ignorados {ignored}");
            Console.ReadKey();
        }
    }
}
