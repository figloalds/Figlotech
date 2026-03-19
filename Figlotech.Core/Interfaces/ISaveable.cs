using System.Threading.Tasks;

namespace Figlotech.Core.Interfaces {
    public interface ISaveable {
        Task<bool> Save();
        Task<bool> Load();
    }
}
