using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.Extensions;

namespace Figlotech.Core.InAppServiceHosting {
    public class FthPipeClient {
        string pipeName;
        public FthPipeClient(string pipeName) {
            this.pipeName = pipeName;
        }

        public string Request(string commands) {
            using (var client = new NamedPipeClientStream(pipeName)) {
                if (client.CanTimeout) {
                    client.WriteTimeout = 3000;
                }

                client.Connect(5000);
                if(!client.IsConnected) {
                    throw new Exception($"No listeners at Pipe {pipeName}");
                }

                client.WriteByte(0x02);
                var reqText = commands;
                var reqBytes = Fi.StandardEncoding.GetBytes(reqText);
                client.Write<int>(reqBytes.Length);
                client.Write(reqBytes, 0, reqBytes.Length);
                client.WriteByte(0x03);

                client.WaitForPipeDrain();

                var init = client.ReadByte();
                var len = client.Read<int>();
                var msg = new byte[len];
                client.Read(msg, 0, msg.Length);
                var msgText = Fi.StandardEncoding.GetString(msg);
                var end = client.ReadByte();
                return msgText;
            }
        }
    }
}
