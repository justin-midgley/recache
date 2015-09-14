﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentAssertions;
using System.Threading;
using ReCache;

namespace Tests
{
	[TestClass]
	public class MainTests
	{
		[TestInitialize()]
		public void Initialize()
		{
		}

		protected string IntLoaderFunc(int key)
		{
			return key.ToString();
		}

		[TestMethod]
		public virtual async Task TestStringAsKey()
		{
			var _backendStore = new Dictionary<string, int>();
			_backendStore.Add("", 0);
			_backendStore.Add("a", 1);
			_backendStore.Add("b", 2);
			_backendStore.Add("abc", 3);

			Func<string, int> loaderFunc = (key) =>
			{
				var val = _backendStore[key];
				// Remove it, so that an exception is thrown if the backend store gets accessed more than once.
				_backendStore.Remove(key);
				return val;
			};

			var _cache = new Cache<string, int>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMinutes(1),
					MaximumCacheSizeIndicator = 10000
				},
				loaderFunc)
				as IAsyncCache<string, int>;

			(await _cache.GetOrLoadAsync("", false)).Should().Be(0);
			(await _cache.GetOrLoadAsync("", false)).Should().Be(0);
			(await _cache.GetOrLoadAsync("a", false)).Should().Be(1);
			(await _cache.GetOrLoadAsync("a", false)).Should().Be(1);
			(await _cache.GetOrLoadAsync("abc", false)).Should().Be(3);
		}

		[TestMethod]
		public virtual async Task TestEnumerableStringAsKeyAndThatLoaderFuncOnlyGetsHitOnce()
		{
			var _backendStore = new Dictionary<IEnumerable<string>, int>(new EnumerableStringComparer());
			_backendStore.Add(Enumerable.Empty<string>(), 0);
			_backendStore.Add(new string[] { "" }, 1);
			_backendStore.Add(new string[] { "a" }, 2);
			_backendStore.Add(new string[] { "a", "b" }, 3);

			Func<IEnumerable<string>, int> loaderFunc = (key) =>
			{
				var val = _backendStore[key];
				// Remove it, so that an exception is thrown if the backend store gets accessed more than once.
				_backendStore.Remove(key);
				return val;
			};

			var _cache = new Cache<IEnumerable<string>, int>(
				new EnumerableStringComparer(),
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMinutes(1),
					MaximumCacheSizeIndicator = 10000
				},
				loaderFunc)
				as IAsyncCache<IEnumerable<string>, int>;

			(await _cache.GetOrLoadAsync(Enumerable.Empty<string>(), false)).Should().Be(0);
			(await _cache.GetOrLoadAsync(new string[] { }, false)).Should().Be(0);
			(await _cache.GetOrLoadAsync(new string[] { "" }, false)).Should().Be(1);
			(await _cache.GetOrLoadAsync(new string[] { "a" }, false)).Should().Be(2);
			(await _cache.GetOrLoadAsync(new string[] { "a", "b" }, false)).Should().Be(3);
		}

		[TestMethod]
		public async Task StaleEntriesShouldBeAutoFlushed()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMilliseconds(100),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;

			Parallel.For(0, 1000, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(1000);
			Thread.Sleep(500);
			_cache.Count.Should().Be(1000);
			Thread.Sleep(700);
			_cache.Count.Should().Be(0);
		}

		[TestMethod]
		public async Task EntriesShouldNotBeFlushedIfExtendWasUsed()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CircuitBreakerTimeoutForAdditionalThreadsPerKey = TimeSpan.FromSeconds(1),
					CacheItemExpiry = TimeSpan.FromMilliseconds(200),
					FlushInterval = TimeSpan.FromMilliseconds(50),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;

			Parallel.For(0, 100, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(100);
			Thread.Sleep(100);
			Parallel.For(0, 50, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(100);
			Thread.Sleep(150);
			Parallel.For(0, 50, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(50);
		}

		[TestMethod]
		public async Task StaleEntriesShouldNotBeFlushedIfFlushIntervalIsNotReached()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromMilliseconds(10),
					FlushInterval = TimeSpan.FromMilliseconds(500),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;

			for (int i = 0; i < 100; i++)
				await _cache.GetOrLoadAsync(i);

			_cache.Count.Should().Be(100);
			Thread.Sleep(600);
			_cache.Count.Should().Be(0);
		}

		[TestMethod]
		public async Task CircuitBreakerShouldOnlyPassThroughFirstThreadRequestAndShouldBlockOtherThreadsAndShareResult()
		{
			var random = new Random();
			int numberOfLoaderCalls = 0;
			var cache = new Cache<int, string>(
				new CacheOptions
				{
					CircuitBreakerTimeoutForAdditionalThreadsPerKey = TimeSpan.MaxValue,
					CacheItemExpiry = TimeSpan.FromMinutes(1),
					FlushInterval = TimeSpan.FromMilliseconds(5000),
					MaximumCacheSizeIndicator = 1000
				},
				(key) =>
				{
					Thread.Sleep(random.Next(50));
					Interlocked.Increment(ref numberOfLoaderCalls);
					return key.ToString();
				});


			int testKey = 7;
			string testValue = testKey.ToString();

			var options = new ParallelOptions() { MaxDegreeOfParallelism = 15 };
			Parallel.For(0, 500, options, (i) =>
			{
				switch (i)
				{
					case 100:
					case 200:
					case 300:
					case 400:
						// Fetch i as the key
						cache.GetOrLoad(i).Should().Be(i.ToString());
						break;
					default: // For all others, fetch the same test key.
						cache.GetOrLoad(testKey).Should().Be(testValue);
						break;
				}
			});

			numberOfLoaderCalls.Should().Be(5, "1 for each of the hundreds (100 to 400), and 1 for the test key");
		}

		[TestMethod]
		public async Task CircuitBreakerShouldOnlyPassThroughFirstThreadRequestAndShouldThrowForOtherThreadsAfterTimeout()
		{
			var random = new Random();
			int numberOfLoaderCalls = 0;

			var cache = new Cache<int, string>(
				new CacheOptions
				{
					CircuitBreakerTimeoutForAdditionalThreadsPerKey = TimeSpan.Zero,
					CacheItemExpiry = TimeSpan.FromMinutes(1),
					FlushInterval = TimeSpan.FromMilliseconds(5000),
					MaximumCacheSizeIndicator = 1000
				},
				(key) =>
				{
					Thread.Sleep(random.Next(5, 50));
					Interlocked.Increment(ref numberOfLoaderCalls);
					return key.ToString();
				});


			int numberOfCacheRequestsShortCircuited = 0;
			int testKey = 7;
			string testValue = testKey.ToString();

			var options = new ParallelOptions() { MaxDegreeOfParallelism = 15 };
			Parallel.For(0, 500, options, (i) =>
			{
				try
				{
					switch (i)
					{
						case 100:
						case 200:
						case 300:
						case 400:
							// Fetch i as the key
							cache.GetOrLoad(i).Should().Be(i.ToString());
							break;
						default: // For all others, fetch the same test key.
							cache.GetOrLoad(testKey).Should().Be(testValue);
							break;
					}
				}
				catch (CircuitBreakerTimeoutException)
				{
					Interlocked.Increment(ref numberOfCacheRequestsShortCircuited);
				}
			});

			numberOfLoaderCalls.Should().Be(5, "1 for each of the hundreds (100 to 400), and 1 for the test key");
			numberOfCacheRequestsShortCircuited.Should().BeGreaterThan(5, "At least some should be too fast (i.e. hit while the key's value is still busy caching), and because the timeout is set to zero, it should fail");
		}

		[TestMethod]
		public async Task EntriesExceedingMaxShouldBeAutoFlushedEvenIfNotStale()
		{
			var cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromMinutes(1),
					FlushInterval = TimeSpan.FromMilliseconds(500),
					MaximumCacheSizeIndicator = 99
				},
				IntLoaderFunc);

			for (int i = 0; i < 200; i++)
				cache.GetOrLoad(i);

			cache.Count.Should().Be(200);
			Thread.Sleep(1500);
			cache.Count.Should().Be(99);
		}

		[TestMethod]
		public async Task TestCacheFlushCallback()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMilliseconds(500),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;

			var flushCallbackRaised = 0;

			_cache.MissedCallback = (key, cacheEntry, durationMilliseconds) =>
			{
				cacheEntry.ClientContext = ((key % 2) == 0) ? "evenContext" : "oddContext";
			};

			_cache.FlushCallback = (count, itemsRemoved, clientContext, millisecondElapsed) =>
			{
				Console.WriteLine("flushCallBackRaised...count :{0}, itemsRemoved: {1}, clientContext : {2}, millisecondElapsed: {3} ", count, itemsRemoved, clientContext, millisecondElapsed);
				flushCallbackRaised++;
			};

			Parallel.For(0, 1000, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(1000);
			Thread.Sleep(500);
			_cache.Count.Should().Be(1000);
			Thread.Sleep(1700);
			_cache.Count.Should().Be(0);
			flushCallbackRaised.Should().Be(4);
		}

		[TestMethod]
		public async Task SelfRefreshingCacheEntriesExceedingMaxShouldBeAutoFlushedEvenIfNotStale()
		{
			var _cache = new SelfRefreshingCache<int, string>(
				new SelfRefreshingCacheOptions
				{
					RefreshInterval = TimeSpan.FromMilliseconds(5),
					StandardCacheOptions = new CacheOptions
					{
						CacheItemExpiry = TimeSpan.FromMinutes(1),
						FlushInterval = TimeSpan.FromMilliseconds(50),
						MaximumCacheSizeIndicator = 99
					}
				},
				IntLoaderFunc)
				as ISelfRefreshingAsyncCache<int, string>;


			for (int i = 0; i < 200; i++)
				await _cache.GetOrLoadAsync(i);

			_cache.Count.Should().Be(200);
			Thread.Sleep(700);
			_cache.Count.Should().Be(99);
		}

		[TestMethod]
		public async Task TryAddToCacheShouldIncreaseItemsInList()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMilliseconds(100),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;


			Parallel.For(0, 5, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(5);

			_cache.TryAdd(6, "6");

			_cache.Count.Should().Be(6);
		}

		[TestMethod]
		public async Task ItemsPropertyShouldReturnListOfItemsInCache()
		{
			var _cache = new Cache<int, string>(
				new CacheOptions
				{
					CacheItemExpiry = TimeSpan.FromSeconds(1),
					FlushInterval = TimeSpan.FromMilliseconds(100),
					MaximumCacheSizeIndicator = 1000
				},
				IntLoaderFunc)
				as IAsyncCache<int, string>;

			Parallel.For(0, 5, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(5);

			_cache.Items.Should().NotBeNull();
			_cache.Items.Count().Should().Be(5);

			_cache.TryAdd(6, "6");

			_cache.Items.Should().NotBeNull();
			_cache.Items.Count().Should().Be(6);
		}

		[TestMethod]
		public async Task SelfRefreshingCacheEntriesTryAddToCacheShouldIncreaseItemsInList()
		{
			var _cache = new SelfRefreshingCache<int, string>(
				new SelfRefreshingCacheOptions
				{
					RefreshInterval = TimeSpan.FromMilliseconds(5),
					StandardCacheOptions = new CacheOptions
					{
						CacheItemExpiry = TimeSpan.FromMinutes(1),
						FlushInterval = TimeSpan.FromMilliseconds(50),
						MaximumCacheSizeIndicator = 99
					}
				},
				IntLoaderFunc)
				as ISelfRefreshingAsyncCache<int, string>;

			Parallel.For(0, 5, async (i) => await _cache.GetOrLoadAsync(i));

			_cache.Count.Should().Be(5);

			_cache.TryAdd(6, "6");

			_cache.Count.Should().Be(6);
		}

		[TestMethod]
		public async Task SelfRefreshingCacheItemsPropertyShouldReturnListOfItemsInCache()
		{
			var _cache = new SelfRefreshingCache<int, string>(
				new SelfRefreshingCacheOptions
				{
					RefreshInterval = TimeSpan.FromMilliseconds(5),
					StandardCacheOptions = new CacheOptions
					{
						CacheItemExpiry = TimeSpan.FromMinutes(1),
						FlushInterval = TimeSpan.FromMilliseconds(50),
						MaximumCacheSizeIndicator = 99
					}
				},
				IntLoaderFunc)
				as ISelfRefreshingAsyncCache<int, string>;

			Parallel.For(0, 5, async (i) => await _cache.GetOrLoadAsync(i));
			_cache.Count.Should().Be(5);

			_cache.Items.Should().NotBeNull();
			_cache.Items.Count().Should().Be(5);

			_cache.TryAdd(6, "6");

			_cache.Items.Should().NotBeNull();
			_cache.Items.Count().Should().Be(6);
		}
	}
}
