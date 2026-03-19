using Figlotech.Extensions;
using System;
using System.IO.Pipes;

namespace Figlotech.Core.InAppServiceHosting {
    public sealed class FthPipeClient {
        readonly string pipeName;
        public FthPipeClient(string pipeName) {
            this.pipeName = pipeName;
        }

        public string Request(string commands) {
            using (var client = new NamedPipeClientStream(pipeName)) {
                if (client.CanTimeout) {
                    client.WriteTimeout = 3000;
                }

                client.Connect(5000);
                if (!client.IsConnected) {
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
