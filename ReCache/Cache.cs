using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Timers;

namespace ReCache
{
	[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix")]
	public class Cache<TKey, TValue> : ICache<TKey, TValue>
	{
		private string _cacheName = "(NotSet)";
		private Random _expiryRandomizer = new Random();
		private bool _isDisposed = false;
		private readonly object _disposeLock = new object();
		public string CacheName
		{
			get { return this._cacheName; }
			set
			{
				if (value == null)
					throw new ArgumentNullException(nameof(value) + " may not be null when setting CacheName");

				this._cacheName = value;
			}
		}

		private readonly ConcurrentDictionary<TKey, KeyGate<TKey>> _keyGates;
		private readonly IKeyValueStore<TKey, TValue> _kvStore;
		private CacheOptions _options;
		private Timer _flushTimer;

		/// <summary>
		/// The function to use for retreaving the entry if it is not yet in the cache.
		/// </summary>
		public Func<TKey, Task<TValue>> LoaderFunction { get; set; }

		/// <summary>
		/// Returns the number of items in the cache by enumerating them (non-locking).
		/// </summary>
		public int Count { get { return this.Items.Count(); } }

		public IEnumerable<KeyValuePair<TKey, TValue>> Items { get { return _kvStore.Entries.Select(x => new KeyValuePair<TKey, TValue>(x.Key, x.Value.CachedValue)); } }

		[Obsolete("Use a constructor that accepts IKeyValueStore<TKey, TValue> instead.")]
		public Cache(
			CacheOptions options)
			: this(options, null)
		{
		}

		[Obsolete("Use a constructor that accepts IKeyValueStore<TKey, TValue> instead.")]
		public Cache(
			CacheOptions options,
			Func<TKey, Task<TValue>> loaderFunction)
		{
			this.SetOptions(options);

			LoaderFunction = loaderFunction;
			_keyGates = new ConcurrentDictionary<TKey, KeyGate<TKey>>();
			_kvStore = new InMemoryKeyValueStore<TKey, TValue>();
			this.InitializeFlushTimer();
		}

		[Obsolete("Use a constructor that accepts IKeyValueStore<TKey, TValue> instead.")]
		public Cache(
			IEqualityComparer<TKey> comparer,
			CacheOptions options)
			: this(comparer, options, null)
		{
		}

		[Obsolete("Use a constructor that accepts IKeyValueStore<TKey, TValue> instead.")]
		public Cache(
			IEqualityComparer<TKey> comparer,
			CacheOptions options,
			Func<TKey, Task<TValue>> loaderFunction)
			: this(options, loaderFunction)
		{
			if (comparer == null)
				throw new ArgumentNullException(nameof(comparer));

			_keyGates = new ConcurrentDictionary<TKey, KeyGate<TKey>>();
			_kvStore = new InMemoryKeyValueStore<TKey, TValue>(comparer);
			this.InitializeFlushTimer();
		}

		public Cache(IKeyValueStore<TKey, TValue> kvStore, CacheOptions options)
			: this(kvStore, options, null)
		{
		}

		public Cache(IKeyValueStore<TKey, TValue> kvStore, CacheOptions options, Func<TKey, Task<TValue>> loaderFunction)
		{
			this.SetOptions(options);

			LoaderFunction = loaderFunction;
			_keyGates = new ConcurrentDictionary<TKey, KeyGate<TKey>>();
			_kvStore = kvStore;
			this.InitializeFlushTimer();
		}

		private void InitializeFlushTimer()
		{
			if (_flushTimer == null)
			{
				_flushTimer = new Timer(_options.FlushInterval.TotalMilliseconds);
				_flushTimer.Elapsed += (sender, eventArgs) =>
				{
					_flushTimer.Stop();
					try
					{
						this.FlushInvalidatedEntries();
					}
					finally
					{
						_flushTimer.Start();
					}
				};
				_flushTimer.Start();
			}
			else
			{
				// The timer was already started by the first constructor, so just stop and restart it,
				// as we have instantiated the dictionary again in the second constructor.
				_flushTimer.Stop();
				_flushTimer.Start();
			}
		}

		private void SetOptions(CacheOptions options)
		{
			if (options == null)
				throw new ArgumentNullException(nameof(options));

			_cacheName = options.CacheName;

			if (_cacheName == null)
				throw new ArgumentNullException(nameof(options.CacheName));
			if (_cacheName.Trim() == string.Empty)
				throw new ArgumentException(nameof(options.CacheName) + " may not be blank or white space");

			options.Initialize();
			_options = options;
		}

		public async Task<TValue> GetOrLoadAsync(
			TKey key)
		{
			return await GetOrLoadAsync(key, false, this.LoaderFunction).ConfigureAwait(false);
		}

		public async Task<TValue> GetOrLoadAsync(
			TKey key,
			Func<TKey, Task<TValue>> loaderFunction)
		{
			return await GetOrLoadAsync(key, false, loaderFunction).ConfigureAwait(false);
		}

		public async Task<TValue> GetOrLoadAsync(
			TKey key,
			bool resetExpiryTimeoutIfAlreadyCached)
		{
			return await GetOrLoadAsync(key, resetExpiryTimeoutIfAlreadyCached, this.LoaderFunction).ConfigureAwait(false);
		}

		public async Task<TValue> GetOrLoadAsync(
			TKey key,
			bool resetExpiryTimeoutIfAlreadyCached,
			Func<TKey, Task<TValue>> loaderFunction)
		{
			TValue v;
			if (this.TryGet(key, resetExpiryTimeoutIfAlreadyCached, out v))
				return v;

			var keyGate = this.EnsureKeyGate(key);
			bool gotKeyLockBeforeTimeout = await keyGate.Lock.WaitAsync(_options.CircuitBreakerTimeoutForAdditionalThreadsPerKey).ConfigureAwait(false);
			if (!gotKeyLockBeforeTimeout)
			{
				throw new CircuitBreakerTimeoutException("CacheName: " + this.CacheName + ". The key's value is already busy loading, but the CircuitBreakerTimeoutForAdditionalThreadsPerKey of {1} ms has been reached. Hitting the cache again with the same key after a short while might work. Key: {0}".FormatWith(key.ToString(), _options.CircuitBreakerTimeoutForAdditionalThreadsPerKey.TotalMilliseconds));
			}
			else // Got the key gate lock.
			{
				try
				{
					return await GetIfCachedAndNotExpiredElseLoad(key, resetExpiryTimeoutIfAlreadyCached, loaderFunction);
				}
				finally
				{
					keyGate.Lock.Release();
				}
			}
		}

		private async Task<TValue> GetIfCachedAndNotExpiredElseLoad(TKey key, bool resetExpiryTimeoutIfAlreadyCached, Func<TKey, Task<TValue>> loaderFunction)
		{
			ICacheEntry<TValue> entry;
			if (_kvStore.TryGetEntry(key, out entry))
			{
				DateTime someTimeAgo = CalculateExpiryTimeStartOffset();
				if (entry.TimeLoaded < someTimeAgo)
				{
					// Entry is stale, reload.
					var newValue = await this.LoadAndCacheEntryAsync(key, loaderFunction).ConfigureAwait(false);
					if (!object.ReferenceEquals(newValue, entry.CachedValue))
						DisposeEntry(entry);

					return newValue;
				}
				else // Cached entry is still good.
				{
					if (resetExpiryTimeoutIfAlreadyCached)
						entry.ResetExpiryTimeout();

					TryHitCallback(key, entry);

					return entry.CachedValue;
				}
			}

			// not in cache at all.
			return (await this.LoadAndCacheEntryAsync(key, loaderFunction).ConfigureAwait(false));
		}

		private void TryHitCallback(TKey key, ICacheEntry<TValue> entry)
		{
			try
			{
				this.HitCallback?.Invoke(key, entry);
			}
			finally { } // suppress client code exceptions
		}

		private DateTime CalculateExpiryTimeStartOffset()
		{
			DateTime someTimeAgo;
			if (_options.CacheItemExpiryPercentageRandomization == 0)
				someTimeAgo = DateTime.UtcNow.AddMilliseconds(-_options.CacheItemExpiry.TotalMilliseconds);
			else
			{
				double ms = _options.CacheItemExpiry.TotalMilliseconds;
				int randomizationWindowMs = _options.CacheItemExpiryPercentageRandomizationMilliseconds;
				double halfRandomizationWindowMs = randomizationWindowMs / 2d;
				// Deduct half of the randomization milliseconds based on the provided percentage.
				ms -= halfRandomizationWindowMs;

				int maxValue = randomizationWindowMs + 1;
				if (maxValue <= 0)
					maxValue = 1;
				ms += _expiryRandomizer.Next(maxValue);

				someTimeAgo = DateTime.UtcNow.AddMilliseconds(-ms);
			}
			return someTimeAgo;
		}

		public TValue Get(TKey key)
		{
			return this.Get(key, false);
		}

		public TValue Get(
			TKey key,
			bool resetExpiryTimeoutIfAlreadyCached)
		{
			ICacheEntry<TValue> entry;
			if (_kvStore.TryGetEntry(key, out entry))
			{
				var someTimeAgo = DateTime.UtcNow.AddMilliseconds(-_options.CacheItemExpiry.TotalMilliseconds);
				if (entry.TimeLoaded < someTimeAgo)
				{
					// Expired
					return default(TValue);
				}

				TryHitCallback(key, entry);

				if (resetExpiryTimeoutIfAlreadyCached)
					entry.ResetExpiryTimeout();
				return entry.CachedValue;
			}
			else // not in cache at all.
				return default(TValue);
		}

		public bool TryGet(
			TKey key,
			bool resetExpiryTimeoutIfAlreadyCached,
			out TValue value)
		{
			ICacheEntry<TValue> entry;
			if (_kvStore.TryGetEntry(key, out entry))
			{
				var someTimeAgo = DateTime.UtcNow.AddMilliseconds(-_options.CacheItemExpiry.TotalMilliseconds);
				if (entry.TimeLoaded < someTimeAgo)
				{
					// Expired
					value = default(TValue);
					return false;
				}

				TryHitCallback(key, entry);

				if (resetExpiryTimeoutIfAlreadyCached)
					entry.ResetExpiryTimeout();

				value = entry.CachedValue;
				return true;
			}
			else // not in cache at all.
			{
				value = default(TValue);
				return false;
			}
		}

		private async Task<TValue> LoadAndCacheEntryAsync(TKey key, Func<TKey, Task<TValue>> loaderFunction)
		{
			if (loaderFunction == null)
				throw new ArgumentNullException(nameof(loaderFunction));

			var stopwatch = System.Diagnostics.Stopwatch.StartNew();

			TValue value = await loaderFunction(key).ConfigureAwait(false);

			ICacheEntry<TValue> entry = _kvStore.AddOrUpdateEntry(key, value, (k, v) => value);

			stopwatch.Stop();

			TryMissedCallback(key, entry, stopwatch.ElapsedMilliseconds);

			return value;
		}

		private void TryMissedCallback(TKey key, ICacheEntry<TValue> entry, long elapsedMilliseconds)
		{
			try
			{
				this.MissedCallback?.Invoke(key, entry, elapsedMilliseconds);
			}
			finally { } // suppress client code exceptions
		}

		private KeyGate<TKey> EnsureKeyGate(TKey key)
		{
			//TODO: make lazy.
			var tempKeyGate = new KeyGate<TKey>(key);
			var keyGate = _keyGates.GetOrAdd(key, (k) => tempKeyGate);
			if (tempKeyGate != keyGate)
				tempKeyGate.Dispose();

			return keyGate;
		}

		public bool Invalidate(TKey key)
		{
			ICacheEntry<TValue> tmp;
			bool removed = _kvStore.TryRemoveEntry(key, out tmp);
			if (removed)
				DisposeEntry(tmp);
			return removed;
		}

		public void InvalidateAll()
		{
			_kvStore.InvalidateAll(this.Invalidate);

			//foreach (var pair in _kvStore.Entries)
			//	Invalidate(pair.Key);
		}

		public bool HasKey(TKey key)
		{
			ICacheEntry<TValue> tmp;
			return _kvStore.TryGetEntry(key, out tmp);
		}

		public void FlushInvalidatedEntries()
		{
			var entriesBeforeFlush = _kvStore.Entries.ToList();
			var stopwatch = new Stopwatch();
			stopwatch.Start();

			var remainingEntriesCount = _kvStore.FlushInvalidatedEntries(_options.MaximumCacheSizeIndicator, 
				DateTime.UtcNow.AddMilliseconds(-_options.CacheItemExpiry.TotalMilliseconds), 
				this.Invalidate);

			stopwatch.Stop();

			int itemsFlushed = entriesBeforeFlush.Count() - remainingEntriesCount;
			TryFlushCallback(remainingEntriesCount, itemsFlushed, stopwatch.ElapsedMilliseconds);
		}

		private void TryFlushCallback(int remainingCount, int itemsFlushed, long elapsedMilliseconds)
		{
			try
			{
				FlushCallback?.Invoke(remainingCount, itemsFlushed, elapsedMilliseconds);
			}
			finally { } // suppress client code exceptions.
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return this.GetEnumerator();
		}

		public IEnumerator<KeyValuePair<TKey, ICacheEntry<TValue>>> GetEnumerator()
		{
			return _kvStore.Entries.GetEnumerator();
		}

		public bool TryAdd(TKey key, TValue value)
		{
			return _kvStore.TryAdd(key, value);
		}

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		~Cache()
		{
			Dispose(false);
		}

		protected virtual void Dispose(bool disposing)
		{
			lock (this._disposeLock)
			{
				if (!_isDisposed)
				{
					if (disposing)
					{
						// free managed resources
						this.InvalidateAll();

						foreach (KeyGate<TKey> keyInfo in _keyGates.Values.Select(ki => ki).ToList())
						{
							keyInfo.Dispose();
							KeyGate<TKey> throwAway;
							_keyGates.TryRemove(keyInfo.Key, out throwAway);
						}

						if (this._flushTimer != null)
						{
							this._flushTimer.Stop();
							this._flushTimer.Dispose();
							this._flushTimer = null;
						}
					}
				}
			}

			// free native resources if there are any.
		}

		private void DisposeEntry(ICacheEntry<TValue> entry)
		{
			if (_options.DisposeExpiredValuesIfDisposable)
			{
				if (entry.CachedValue is IDisposable)
				{
					var val = (IDisposable)entry.CachedValue;
					val.Dispose();
				}
			}
		}

		public Action<TKey, ICacheEntry<TValue>> HitCallback { get; set; }

		/// <summary>
		/// The long parameter is the duration in ms.
		/// </summary>
		public Action<TKey, ICacheEntry<TValue>, long> MissedCallback { get; set; }

		/// <summary>
		/// The 3 long parameters of the action are:
		/// Remaining entry count.
		/// Number of Items flushed.
		/// Duration in ms the flush op took.
		/// </summary>
		public Action<int, int, long> FlushCallback { get; set; }
	}
}