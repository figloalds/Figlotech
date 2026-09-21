namespace Figlotech.Core.Interfaces {
    /// <summary>A custom binary value. Implementations must keep their wire representation stable.</summary>
    public interface IBinarySerializable {
        /// <summary>Returns the payload without a length prefix. Must not return null.</summary>
        byte[] ToBytes();

        /// <summary>Replaces this instance's state from the payload; reject invalid data.</summary>
        void FromBytes(byte[] bytes);

        /// <summary>Whether every non-null payload has exactly Length bytes. Defaults to false.</summary>
        static virtual bool IsFixedLength => false;

        /// <summary>Non-negative payload size when IsFixedLength is true; otherwise ignored.</summary>
        static virtual int Length => 0;
    }
}
