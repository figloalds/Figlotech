using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core.Extensions
{
    public static class ArraySegmentExtensions
    {

        public static T[] ToSegmentArray<T>(this ArraySegment<T> arraySegment) {
            T[] array = new T[arraySegment.Count];
            Array.Copy(arraySegment.Array, arraySegment.Offset, array, 0, arraySegment.Count);
            return array;
        }
    }
}
