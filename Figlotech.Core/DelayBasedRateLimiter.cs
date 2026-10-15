using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    /// <summary>
    /// A thread-safe token bucket that delays callers instead of rejecting requests.
    /// Reuse one instance for each user or group of callers that shares a rate limit.
    /// </summary>
    public sealed class DelayBasedRateLimiter {
        private const double MaximumDelayChunkSeconds = int.MaxValue / 1000.0;
        private const double MinimumPenaltySeconds = 0.05;
        private readonly object _sync = new object();
        private readonly int _maxRequests;
        private readonly double _windowSeconds;
        private readonly double _maxDelaySeconds;
        private readonly long _origin;
        // Times are seconds since construction on a virtual timeline: each reservation is scheduled
        // as if its caller arrived right after the previous caller was released. _lastUpdate and
        // _nextRelease may therefore lie in the future while earlier reservations are still waiting.
        private double _tokens;
        private double _lastUpdate;
        private double _nextRelease;
        private long _lastReservationId;

        /// <param name="maxRequests">Bucket capacity and number of tokens refilled per window.</param>
        /// <param name="windowSeconds">Positive, finite refill window in seconds.</param>
        /// <param name="maxDelay">
        /// Nonnegative maximum progressive penalty; null defaults to five seconds. This does not cap
        /// the time spent waiting for a token when the bucket is empty.
        /// </param>
        public DelayBasedRateLimiter(int maxRequests = 30, double windowSeconds = 60, TimeSpan? maxDelay = null) {
            if (maxRequests <= 0) {
                throw new ArgumentOutOfRangeException(nameof(maxRequests), "Capacity must be positive.");
            }
            if (!double.IsFinite(windowSeconds) || windowSeconds <= 0) {
                throw new ArgumentOutOfRangeException(nameof(windowSeconds), "Window must be positive and finite.");
            }
            var effectiveMaxDelay = maxDelay ?? TimeSpan.FromSeconds(5);
            if (effectiveMaxDelay < TimeSpan.Zero) {
                throw new ArgumentOutOfRangeException(nameof(maxDelay), "Maximum delay must be nonnegative.");
            }

            _maxRequests = maxRequests;
            _windowSeconds = windowSeconds;
            _maxDelaySeconds = effectiveMaxDelay.TotalSeconds;
            _tokens = maxRequests;
            _origin = Stopwatch.GetTimestamp();
        }

        /// <summary>
        /// Waits for a token, consumes it, then applies a cubic utilization penalty.
        /// Penalties below 50 milliseconds are skipped. No thread is blocked while waiting.
        /// </summary>
        /// <remarks>
        /// Concurrent callers are released in arrival order, each as if it had called right after the
        /// previous caller returned, so parallel calls pay the same penalties as sequential ones.
        /// Each caller reserves its release time up front and sleeps once.
        /// Cancellation interrupts either wait. A canceled caller that had not yet reached its token
        /// gives it back; a token already consumed before the progressive penalty is not refunded.
        /// </remarks>
        public async ValueTask WaitAsync(CancellationToken cancellationToken = default) {
            Reservation reservation;
            lock (_sync) {
                cancellationToken.ThrowIfCancellationRequested();
                reservation = Reserve(Now());
            }

            try {
                while (true) {
                    var remainingSeconds = reservation.ReleaseTime - Now();
                    if (remainingSeconds <= 0) {
                        return;
                    }
                    // Round tiny waits up to avoid spinning, and chunk long ones so every finite
                    // configuration is safe for Task.Delay.
                    await Task.Delay(TimeSpan.FromSeconds(
                        Math.Clamp(remainingSeconds, 0.001, MaximumDelayChunkSeconds)), cancellationToken);
                }
            } catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) {
                lock (_sync) {
                    Cancel(reservation, Now());
                }
                throw;
            }
        }

        private double Now() => Stopwatch.GetElapsedTime(_origin).TotalSeconds;

        private Reservation Reserve(double now) {
            var previousTokens = _tokens;
            var previousLastUpdate = _lastUpdate;
            var previousNextRelease = _nextRelease;

            var start = Math.Max(now, _nextRelease);
            Refill(start);
            var tokenTime = start;
            if (_tokens < 1.0) {
                // Clamp so extreme configurations saturate instead of reaching infinity, which
                // would turn later elapsed-time arithmetic into NaN.
                tokenTime = Math.Min(start + (1.0 - _tokens) / _maxRequests * _windowSeconds, double.MaxValue);
                _tokens = 1.0;
                _lastUpdate = tokenTime;
            }
            _tokens -= 1.0;

            var utilization = (_maxRequests - _tokens) / _maxRequests;
            var penaltySeconds = utilization * utilization * utilization * _maxDelaySeconds;
            if (penaltySeconds < MinimumPenaltySeconds) {
                penaltySeconds = 0;
            }
            _nextRelease = Math.Min(tokenTime + penaltySeconds, double.MaxValue);

            return new Reservation(++_lastReservationId, tokenTime, _nextRelease, _tokens,
                previousTokens, previousLastUpdate, previousNextRelease);
        }

        private void Refill(double time) {
            _tokens = Math.Min(_maxRequests, _tokens + (time - _lastUpdate) / _windowSeconds * _maxRequests);
            _lastUpdate = time;
        }

        private void Cancel(Reservation reservation, double now) {
            var isLatest = reservation.Id == _lastReservationId;
            if (now < reservation.TokenTime) {
                if (isLatest) {
                    // Nothing was scheduled after this caller: undo its reservation, keeping any
                    // tokens that earlier canceled callers returned in the meantime.
                    var returnedSince = _tokens - reservation.TokensAfter;
                    _tokens = Math.Min(_maxRequests, reservation.PreviousTokens + returnedSince);
                    _lastUpdate = reservation.PreviousLastUpdate;
                    _nextRelease = reservation.PreviousNextRelease;
                } else {
                    // Later callers keep their schedule; the token goes to the next new caller.
                    _tokens = Math.Min(_maxRequests, _tokens + 1.0);
                }
            } else if (isLatest) {
                // The consumed token is not refunded, but new callers need not wait out the
                // abandoned penalty.
                _nextRelease = Math.Min(_nextRelease, now);
            }
        }

        private readonly record struct Reservation(
            long Id,
            double TokenTime,
            double ReleaseTime,
            double TokensAfter,
            double PreviousTokens,
            double PreviousLastUpdate,
            double PreviousNextRelease);
    }
}
