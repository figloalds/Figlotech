using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using Newtonsoft.Json;

namespace Figlotech.Networking
{
    class NetRequests
    {
        const int maxattempts = 5;
        public static bool CheckLink(string url, string args = "")
        {
            try
            {
                String reqstr = String.Format(url, args);
                Uri uriResult;
                bool result = Uri.TryCreate(reqstr, UriKind.Absolute, out uriResult) && uriResult.Scheme == Uri.UriSchemeHttp;
                return result;
            }
            catch (Exception) {
                return false;
            }
        }

        public static string Post(string url, object requestBody)
        {
            int attempts = maxattempts;
            do
            {
                try
                {
                    var content = JsonConvert.SerializeObject(requestBody);
                    HttpWebRequest pesq = (HttpWebRequest)WebRequest.CreateHttp(url);
                    pesq.Method = "POST";
                    pesq.ContentType = "application/json";
                    pesq.ContentLength = content.Length;
                    StreamWriter stOut = new StreamWriter(pesq.GetRequestStream(), Encoding.ASCII);
                    stOut.Write(content);
                    stOut.Close();
                    StreamReader stIn = new StreamReader(pesq.GetResponse().GetResponseStream());
                    String buffer = stIn.ReadToEnd();
                    stIn.Close();
                    return buffer;
                }
                catch (Exception)
                {
                    Console.WriteLine("FALHA!");
                    Console.WriteLine("Tentando novamente em dentro de 3s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                    Console.WriteLine("Tentando novamente em dentro de 2s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                    Console.WriteLine("Tentando novamente em dentro de 1s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 2);
                }
            } while (attempts-- > 0);
            return null;
        }

        public static string Get(String url)
        {
            int attempts = maxattempts;
            do
            {
                try
                {
                    String reqstr = url;
                    HttpWebRequest main = (HttpWebRequest)WebRequest.CreateHttp(reqstr);
                    main.Method = "GET";
                    //main.ContentType = "application/x-www-form-urlencoded";
                    StreamReader stIn2 = new StreamReader(main.GetResponse().GetResponseStream());
                    String buffer = stIn2.ReadToEnd();
                    stIn2.Close();
                    return buffer;
                    //Console.WriteLine(url);
                }
                catch (Exception)
                {
                    Console.WriteLine("FALHA!");
                    Console.WriteLine("Tentando novamente em dentro de 3s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                    Console.WriteLine("Tentando novamente em dentro de 2s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 1);
                    Console.WriteLine("Tentando novamente em dentro de 1s.");
                    System.Threading.Thread.Sleep(1000);
                    Console.SetCursorPosition(0, Console.CursorTop - 2);
                }
            } while (attempts-- > 0);
            return null;
        }
    }
}
