using Microsoft.VisualStudio.TestTools.UnitTesting;
using FluentAssertions;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace ReCache.Redis.Tests
{
	[TestClass]
	public class RedisKeyValueStoreTests
	{
		private const string _serverAddress = "127.0.0.1";
		private const int _serverPort = 6379;

		private static ConnectionMultiplexer GetTestConnection()
		{
			ConfigurationOptions serverConfig = new ConfigurationOptions();
			serverConfig.EndPoints.Add(System.Net.IPAddress.Parse(_serverAddress), _serverPort);

			ConnectionMultiplexer connection = ConnectionMultiplexer.Connect(serverConfig);

			return connection;
		}

		[TestMethod]
		public void NumberOfTotalRedisKeysShouldBeCorrect()
		{
			int numberOfKeys = 100;

			using (ConnectionMultiplexer connection = GetTestConnection())
			{
				IServer server = connection.GetServer(_serverAddress, _serverPort);

				RedisKeyValueStore<int, int> kvStore = new RedisKeyValueStore<int, int>(server, -1,  TimeSpan.FromSeconds(5.0), "keyCountTest_", (s => int.Parse(s)));

				for (int i = 0; i < numberOfKeys; i++)
				{
					if (!kvStore.TryAdd(i, i))
					{
						numberOfKeys--;
					}
				}

				kvStore.Entries.Count().Should().Be(numberOfKeys);
			}
		}

		[TestMethod]
		public void RedisKeysShouldExpire()
		{
			using (ConnectionMultiplexer connection = GetTestConnection())
			{
				IServer server = connection.GetServer(_serverAddress, _serverPort);

				RedisKeyValueStore<int, int> kvStore = new RedisKeyValueStore<int, int>(server, -1, TimeSpan.FromSeconds(1.0), "keyExpiryTest_", (s => int.Parse(s)));

				Cache<int, int> cache = new Cache<int, int>(kvStore, new CacheOptions() { CacheItemExpiry = TimeSpan.FromSeconds(1.0) });

				for (int i = 0; i < 10; i++)
				{
					if (cache.TryAdd(1, 1))
						break;

					if (i == 9)
					{
						throw new Exception("Could not add test key to cache. ");
					}
				}

				int temp;
				cache.TryGet(1, false, out temp).Should().BeTrue();

				Task.Delay(TimeSpan.FromSeconds(2.0)).Wait();

				cache.TryGet(1, false, out temp).Should().BeFalse();
			}
		}

		[TestMethod]
		public void RedisKeyValueStoreShouldAcceptPrimitiveTypes()
		{
			using (ConnectionMultiplexer connection = GetTestConnection())
			{
				IServer server = connection.GetServer(_serverAddress, _serverPort);

				RedisKeyValueStore<int, object> store1 = new RedisKeyValueStore<int, object>(server, -1,  TimeSpan.Zero, "");

				RedisKeyValueStore<string, object> store2 = new RedisKeyValueStore<string, object>(server, -1, TimeSpan.Zero, "");

				RedisKeyValueStore<long, object> store3 = new RedisKeyValueStore<long, object>(server, -1, TimeSpan.Zero, "");
			}
		}

		[TestMethod]
		public void RedisKeyValueStoreShouldRejectNonPrimitiveType()
		{
			using (ConnectionMultiplexer connection = GetTestConnection())
			{
				IServer server = connection.GetServer(_serverAddress, _serverPort);

				bool exception = false;
				try
				{
					RedisKeyValueStore<List<string>, object> store = new RedisKeyValueStore<List<string>, object>(server, -1, TimeSpan.Zero, "");
				}
				catch (ArgumentException ex)
				{
					// Make sure we got the correct exception
					if (ex.Message.Contains("non-primitive TKey"))
					{
						exception = true;
					}
				}

				if (!exception)
					throw new Exception("Non-primitive type was accepted.");
			}
		}

		[TestMethod]
		public void RedisKeyValueStoreShouldRejectLackOfToStringImplementation()
		{
			using (ConnectionMultiplexer connection = GetTestConnection())
			{
				IServer server = connection.GetServer(_serverAddress, _serverPort);

				bool exception = false;
				try
				{
					RedisKeyValueStore<List<string>, object> store = new RedisKeyValueStore<List<string>, object>(server, -1,TimeSpan.Zero, "", (s) => new List<string>());
				}
				catch (ArgumentException ex)
				{
					// Make sure we got the correct exception
					if (ex.Message.Contains("ToString"))
					{
						exception = true;
					}
				}

				if (!exception)
					throw new Exception("Type without ToString implementation was accepted.");
			}
		}
	}
}
