using Figlotech.Autokryptex;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Drawing;

namespace Iaetec.Captcha {
    public class CaptchaStore
    {
        public String Codigo;
        public String Solucao;
    }

    public class Captcha {
        public String Codigo;
        public String Solucao;
        public Image Imagem;
        public static int _sizeofCaptcha = 6;
        public static Size _captchaSize = new Size(600, 200);
        public static CrossRandom r = new CrossRandom(Int32.MaxValue ^ String.Empty.GetHashCode());
        public static int paddings = 8;
        public static float UsableWidth { get { return _captchaSize.Width - (paddings * 2); } }
        public static float VerticalMid { get { return (_captchaSize.Height / 2)-(fontSize/2); } }
        public static int fontSize = 79;
        public const String captchaChars = "ABCDEFGHJKMNPRSTUVWXYZ23456789";
        public static List<CaptchaStore> _memCaptchas = new List<CaptchaStore>();
        private static Font[] _rFonts = new Font[] {
            new Font(FontFamily.GenericMonospace, fontSize),
            new Font(FontFamily.GenericSansSerif, fontSize),
            //new Font(FontFamily.GenericSerif, fontSize),
            new Font(FontFamily.GenericMonospace, fontSize),
            new Font(FontFamily.GenericSansSerif, fontSize),
            //new Font(FontFamily.GenericSerif, fontSize),
            new Font(FontFamily.GenericMonospace, fontSize, FontStyle.Bold),
            new Font(FontFamily.GenericSansSerif, fontSize, FontStyle.Bold),
            //new Font(FontFamily.GenericSerif, fontSize, FontStyle.Bold),
        };
        private static Color SRandyColor {
            get {
                var col = r.Next(6);
                switch (col) {
                    case 0:
                        return Color.Cyan;
                    case 1:
                        return Color.Magenta;
                    case 2:
                        return Color.Yellow;
                    case 3:
                        return Color.Red;
                    case 4:
                        return Color.Green;
                    case 5:
                        return Color.Blue;
                    default:
                        return Color.FromArgb(150 + r.Next(35), r.Next(150), r.Next(150), r.Next(150));
                }
            }
        }

        private static Color RRandyColor {
            get {
                return Color.FromArgb(255, r.Next(256), r.Next(256), r.Next(256));
            }
        }

        private static Color RandyColor {
            get {
                var col = r.Next(3);
                switch (col)
                {
                    case 0:
                        return Color.FromArgb(150 + r.Next(35), r.Next(255), 0, 0);
                    case 1:
                        return Color.FromArgb(150 + r.Next(35), 0, r.Next(255), 0);
                    case 2:
                        return Color.FromArgb(150 + r.Next(35), 0, 0, r.Next(255));
                    case 3:
                        return Color.FromArgb(150 + r.Next(35), 0, r.Next(150), r.Next(255));
                    case 4:
                        return Color.FromArgb(150 + r.Next(35), r.Next(150), 0, r.Next(255));
                    case 5:
                        return Color.FromArgb(150 + r.Next(35), r.Next(255), r.Next(150), 0);
                    default:
                        return Color.FromArgb(150 + r.Next(35), r.Next(150), r.Next(150), r.Next(150));
                }
            }
        }
        private static Color LRandyColor {
            get {
                switch (r.Next(3))
                {
                    case 0:
                        return Color.FromArgb(200 + r.Next(35), 255, r.Next(150), 0);
                    case 1:
                        return Color.FromArgb(200 + r.Next(35), 0, r.Next(150), 255);
                    case 2:
                        return Color.FromArgb(200 + r.Next(35), r.Next(150), 0, 255);
                    default:
                        return Color.FromArgb(200 + r.Next(35), r.Next(150), r.Next(150), r.Next(150));
                }
            }
        }
        private static Color RandyInvColor {
            get {
                var c = RandyColor;
                return Color.FromArgb(c.A, 255 - c.R, 255 - c.G, 255 - c.B);
            }
        }

        internal Captcha() {
            Codigo = GerarCodigo();
        }
        private static Color noiseColor(Color c, float pct) {
            var retv = Color.FromArgb(
                c.A,
                (int) noiseNumber(c.R, pct),
                (int) noiseNumber(c.G, pct),
                (int) noiseNumber(c.B, pct));
            return retv;
        }
        private static float noiseNumber(float input, float pct) {
            input *= 10000;
            int diff = (int) (input * (pct / 100));
            if (diff > 0) {
                input += r.Next(diff * 2);
                input -= diff;
            }
            return input / 10000;
        }

        public static bool Validar(String Token, String Solucao) {
            var find = _memCaptchas.Find((a) => a.Codigo == Token && a.Solucao == Solucao);
            return find != null;
        }

        public static Image GerarImg(String Solucao) {
            Bitmap retvImg = new Bitmap(_captchaSize.Width, _captchaSize.Height);
            //for (int x = 0; x < retvImg.Width; x++)
            //{
            //    for (int y = 0; y < retvImg.Height; y++)
            //    {
            //        retvImg.SetPixel(x, y, RandyColor);
            //    }
            //}
            Graphics g = Graphics.FromImage(retvImg);
            g.Clear(Color.White);
            //for (int x = 0; x < retvImg.Width; x++) {
            //    for (int y = 0; y < retvImg.Height; y++) {
            //        //if (r.Next(10000) > 9800)
            //        //    g.DrawRectangle(new Pen(RandyColor), x, y, 1, 1);
            //        //if (r.Next(10000) > 9900)
            //        //{
            //        //    g.DrawString(IntEx.Alphanumeric[r.Next(IntEx.Alphanumeric.Length)].ToString(), new Font(FontFamily.GenericMonospace, 12), new SolidBrush(RandyColor), new PointF((float)r.Next(retvImg.Width), (float)r.Next(retvImg.Height)));
            //        //}
            //        if (r.Next(10000) > 9900) {
            //            g.DrawString(IntEx.Alphanumeric[r.Next(IntEx.Alphanumeric.Length)].ToString(), new Font(FontFamily.GenericMonospace, 22), new SolidBrush(RandyInvColor), new PointF((float)r.Next(retvImg.Width), (float)r.Next(retvImg.Height)));
            //        }
            //    }
            //}
            //for (int x = 0; x < retvImg.Width; x++) {
            //    for (int y = 0; y < retvImg.Height; y++) {
            //        g.RotateTransform(-6 + r.Next(12));
            //        if (r.Next(10000) > 9800)
            //            g.FillRectangle(new SolidBrush(RandyColor), new Rectangle(x, y, (int)noiseNumber(r.Next(15), 50), 1));
            //        if (r.Next(10000) > 9800)
            //            g.FillRectangle(new SolidBrush(RandyColor), new Rectangle(x, y, 1, (int)noiseNumber(r.Next(15), 50)));
            //        if (r.Next(10000) > 9800)
            //            g.FillRectangle(new SolidBrush(RandyInvColor), new Rectangle(x, y, (int)noiseNumber(r.Next(15), 50), 1));
            //        if (r.Next(10000) > 9800)
            //            g.FillRectangle(new SolidBrush(RandyInvColor), new Rectangle(x, y, 1, (int)noiseNumber(r.Next(15), 50)));
            //        if (r.Next(10000) > 8000)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyColor)), new Rectangle(x, y, (int)noiseNumber(5, 50), 1));
            //        if (r.Next(10000) > 8000)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyColor)), new Rectangle(x, y, 1, (int)noiseNumber(5, 50)));
            //        if (r.Next(10000) > 8000)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyInvColor)), new Rectangle(x, y, (int)noiseNumber(5, 50), 1));
            //        if (r.Next(10000) > 8000)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyInvColor)), new Rectangle(x, y, 1, (int)noiseNumber(5, 50)));
            //        g.ResetTransform();
            //    }
            //}

            //for (int x = blocksz/2; x < retvImg.Width - blocksz; x += blocksz)
            //{
            //    for (int y = blocksz / 2; y < retvImg.Height - blocksz; y += blocksz)
            //    {
            //        var cor = RandyInvColor;
            //        for (int x1 = x; x1 < x + blocksz; x1++)
            //        {
            //            for (int y1 = y; y1 < y + blocksz; y1++)
            //            {
            //                retvImg.SetPixel(x1, y1, cor);
            //            }
            //        }
            //    }
            //};

            for (int i = 0; i < Solucao.Length; i++)
            {
                g.RotateTransform(-2 + r.Next(4));
                var font = _rFonts[r.Next(_rFonts.Length)];
                g.TranslateTransform(noiseNumber(i * (UsableWidth / _sizeofCaptcha), 2), noiseNumber((float)VerticalMid, 5));
                //for(int o = 0; o < 6; o++)
                //{
                //    g.DrawString(Solucao[i].ToString(), font, new SolidBrush(RandyInvColor), new PointF(noiseNumber(2, 50), noiseNumber(2, 50)));
                //}
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(0, 0));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(0, 0));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(0, 0));
                g.DrawString(Solucao[i].ToString(), font, new SolidBrush(Color.Black), new PointF(0, 0));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(RandyInvColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(RandyInvColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(Solucao[i].ToString(), font, new SolidBrush(LRandyColor), new PointF(noiseNumber(5, 50), noiseNumber(5, 50)));
                //g.DrawString(retv.Solucao[i].ToString(), _rFonts[r.Next(_rFonts.Length)], new SolidBrush(RandyColor), new PointF( noiseNumber(i * (UsableWidth / _sizeofCaptcha), 3), noiseNumber((float) VerticalMid,3) )  );
                g.ResetTransform();
            }
            //for (int x = 0; x < retvImg.Width; x++)
            //{
            //    for (int y = 0; y < retvImg.Height; y++)
            //    {
            //        //if (r.Next(10000) > 9800)
            //        //    g.FillRectangle(new SolidBrush(RandyColor), new Rectangle(x, y, (int)noiseNumber(r.Next(15), 50), 1));
            //        //if (r.Next(10000) > 9800)
            //        //    g.FillRectangle(new SolidBrush(RandyInvColor), new Rectangle(x, y, (int)noiseNumber(r.Next(15), 50), 1));
            //        if (r.Next(10000) > 8800)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyColor)), new Rectangle(x, y, (int)noiseNumber(5, 50), 1));
            //        if (r.Next(10000) > 8800)
            //            g.FillRectangle(new SolidBrush(Color.FromArgb(50, RandyInvColor)), new Rectangle(x, y, 1, (int)noiseNumber(5, 50)));
            //    }
            //}
            g.Flush();
            g.Save();
            int seed = r.Next(100000000);
            List<Color> fore = new List<Color>();
            List<Color> back = new List<Color>();
            for(int i = 0; i < 4; i++) {
                fore.Add(RRandyColor);
                back.Add(RRandyColor);
                fore.Add(Color.Black);
                back.Add(Color.White);
            }
            fore.Add(Color.Black);
            fore.Add(Color.White);
            back.Add(Color.Black);
            back.Add(Color.White);
            //for (int y = 0; y < retvImg.Height; y++) {
            //    Random r2 = new Random(seed);
            //    for (int x = 0; x < retvImg.Width; x++) {
            //        //if (r2.Next(10000) > 9500)
            //        //{
            //        //    if (r2.Next(10000) > 8000)
            //        //        retvImg.SetPixel(x, y, Color.FromArgb(12, Color.Black));
            //        //    else
            //        //        retvImg.SetPixel(x, y, Color.FromArgb(12, Color.White));
            //        //    continue;
            //        //}
            //        var c = retvImg.GetPixel(x, y);
            //        if (r2.Next(10000) > 9500) {
            //            int filter = r.Next(10);
            //            retvImg.SetPixel(x, y, Color.FromArgb(255, r.Next(255), r.Next(255), r.Next(255)));
            //            continue;
            //        }
            //    }
            //}
            Bitmap bp = new Bitmap(retvImg);
            for (var x = 0; x < retvImg.Width; x++) {
                for (var y = 0; y < retvImg.Height; y++) {
                    var col = bp.GetPixel(x, y);
                    int l = (col.R + col.G + col.B) / 3;
                    if (l <= 128) {
                        var c1 = fore[(x + y) % fore.Count];
                        var c2 = fore[r.Next(fore.Count)];
                        var c3 = Color.FromArgb(255,
                            (c1.R + c2.R) / 2,
                            (c1.G + c2.G) / 2,
                            (c1.B + c2.B) / 2);
                        bp.SetPixel(x, y, c3);
                    } else {
                        var c1 = back[(x + y) % fore.Count];
                        var c2 = back[r.Next(fore.Count)];
                        var c3 = Color.FromArgb(255,
                            (c1.R + c2.R) / 2,
                            (c1.G + c2.G) / 2,
                            (c1.B + c2.B) / 2);
                        bp.SetPixel(x, y, c3);
                    }
                    var cl1 = bp.GetPixel(x, y);
                    var cl2 = RRandyColor;
                    var cl3 = Color.FromArgb(255,
                        (cl1.R + cl2.R) / 2,
                        (cl1.G + cl2.G) / 2,
                        (cl1.B + cl2.B) / 2);
                    bp.SetPixel(x, y, cl3);
                }
            }
            retvImg = bp;
            for (int x = 0; x < retvImg.Width; x++) {
                Random r2 = new Random(seed);
                for (int y = 0; y < retvImg.Height; y++) {
                    //if (r2.Next(10000) > 9500)
                    //{
                    //    if (r2.Next(10000) > 8000)
                    //        retvImg.SetPixel(x, y, Color.FromArgb(12, Color.Black));
                    //    else
                    //        retvImg.SetPixel(x, y, Color.FromArgb(12, Color.White));
                    //    continue;
                    //}
                    var c = retvImg.GetPixel(x, y);
                    if (r2.Next(10000) > 9500) {
                        int filter = r.Next(10);
                        retvImg.SetPixel(x, y, Color.FromArgb(255, r.Next(255), r.Next(255), r.Next(255)));
                        continue;
                    }
                }
            }

            int blocksz = 26;
            for (int i = 0; i < 4; i++) {
                blocksz += blocksz * ((500 + r.Next(500)) / 1000);
                for (int x = 0; x < retvImg.Width; x += blocksz) {
                    for (int y = 0; y < retvImg.Height; y += blocksz) {
                        if (r.Next(10000) > 0) {
                            int filter = 1; // 3 + r.Next(2);
                            //if (r.Next(10000) > 7000)
                            filter = r.Next(3);
                            //filter = r.Next(3);
                            //if (i > 1)
                            //    filter = 3 + r.Next(3);
                            for (int x1 = x; x1 < x + blocksz && x1 < retvImg.Width; x1++) {
                                Random r2 = new Random(seed);
                                for (int y1 = y; y1 < y + blocksz && y1 < retvImg.Height; y1++) {
                                    //if (x1 == x || y1 == y || y1 == y + blocksz - 1 || x1 == x + blocksz - 1)
                                    //    retvImg.SetPixel(x1, y1, Color.White);
                                    var c = retvImg.GetPixel(x1, y1);
                                    retvImg.SetPixel(x1, y1, Color.FromArgb(255, c.B, c.R, c.B));
                                    switch (filter) {
                                        case 0:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, 255 - c.R, 255 - c.G, 255 - c.B));
                                            break;
                                        case 1:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, 255 - c.G, 255 - c.B, 255 - c.R));
                                            break;
                                        case 2:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, 255 - c.B, 255 - c.R, 255 - c.G));
                                            break;
                                        case 3:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, c.B, c.G, c.R));
                                            break;
                                        case 4:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, c.G, c.B, c.R));
                                            break;
                                        case 5:
                                            retvImg.SetPixel(x1, y1, Color.FromArgb(255, c.B, c.R, c.G));
                                            break;
                                    }
                                }
                            }
                        }
                    }
                };
            }

            return retvImg;
        }

        public static Captcha GerarNovo() {
            Captcha retv = new Captcha();
            char[] novaRes = new char[_sizeofCaptcha];
            for(int i = 0; i < novaRes.Length; i++) {
                novaRes[i] = captchaChars[r.Next(captchaChars.Length)];
            }
            retv.Solucao = new String(novaRes);
            retv.Imagem = GerarImg(retv.Solucao);
            return retv;
        }

        private String GerarCodigo() {
            IntEx codigo = new IntEx(DateTime.Now.Ticks);
            codigo *= 1000000;
            codigo += new Random().Next(1000000);
            return codigo.ToString(IntEx.Base36);
        }
    }
}
