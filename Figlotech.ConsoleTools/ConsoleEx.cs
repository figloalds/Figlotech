
namespace System {
    public static class ConsoleEx
    {
        public static T GetInfo<T>(String infoName) {
            // labels, seriusly?
            // é, parece valido para apps de console, no pro.
            var cursorPos = Console.CursorTop;
            start:
            Console.WriteLine();
            Console.WriteLine();
            Console.CursorLeft = 0;
            Console.WriteLine($"Insert value for '{infoName}':");
            var val = Console.ReadLine();
            yesno:
            Console.WriteLine($"Value inserted: '{val}', Correct? Y/N");
            var resp = Console.ReadKey();
            switch (resp.Key) {
                case ConsoleKey.Y:
                    try {
                        T retv = (T)Convert.ChangeType(val, typeof(T));
                        while (Console.CursorTop > cursorPos) {
                            int currentLineCursor = Console.CursorTop;
                            Console.SetCursorPosition(0, Console.CursorTop);
                            Console.Write(new string(' ', Console.WindowWidth));
                            Console.SetCursorPosition(0, currentLineCursor);
                            Console.CursorTop--;
                        }
                        Console.Write(new string(' ', Console.WindowWidth));
                        Console.CursorLeft = 0;
                        Console.CursorTop--;
                        Console.WriteLine($"{infoName} = {val}");
                        return retv;
                    } catch (Exception x) {
                        Console.WriteLine($"Não é possivel converter {val} em {typeof(T).Name}");
                    }
                    break;
                case ConsoleKey.N:
                    goto start;
                default:
                    goto yesno;
            }

            return default(T);
        }
    }
}
