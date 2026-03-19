namespace Figlotech.Core.Interfaces {
    public interface IContinuousExecutor {
        void Start();
        void Stop(bool wait);
    }
}
