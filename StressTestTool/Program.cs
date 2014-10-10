using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace StressTestTool
{
	class Program
	{
		private static int successfulQueries, failedQueries;
		private static readonly object syncRoot = new object();

		// Default thread pool
		private static Action<WaitCallback> enqueue = callback => ThreadPool.QueueUserWorkItem(callback);
		private static Func<int> getFreeThreads = () =>
		{
			int workerThreads, completionPortThreads;
			ThreadPool.GetAvailableThreads(out workerThreads, out completionPortThreads);
			//			ThreadPool.GetMaxThreads(out workerThreads, out completionPortThreads);
			return workerThreads;
		};

		private static Action execute = () => SimpleQuery.RunQuery();

		static void Main(string[] args)
		{
			//ThreadPool.SetMaxThreads(2048, 2048);
			//ThreadPool.SetMinThreads(128, 128);

			var myThreadPool = new SimpleThreadPool(4);
			enqueue = myThreadPool.QueueUserWorkItem;
			getFreeThreads = myThreadPool.GetFreeThreads;

			//execute = RemoteCall.Execute;

			Console.WriteLine("Running {0}", typeof(NpgsqlConnection).Assembly.FullName);

			var timer = Stopwatch.StartNew();
			var randomEnabled = false;
			var rand = new Random();
			var lastMessage = "";
			while (true)
			{
				Thread.Sleep(500);

				var openConnections = GetOpenConnections(5432, Process.GetCurrentProcess().Id);

				lock (syncRoot)
				{
					var newMessage = String.Format("Queries: success={0}, fail={1}  Connections: {2}  Available threads: {3}  Query time: min={4:F3}ms, max={5:F3}ms, avg={6:F3}ms",
						successfulQueries, failedQueries,
						openConnections, getFreeThreads(),
						minElapsedTime.TotalMilliseconds, maxElapsedTime.TotalMilliseconds, totalElapsedTime.TotalMilliseconds / successfulQueries);

					if (newMessage != lastMessage)
					{
						Console.WriteLine("\b{0:F3} {1}", timer.Elapsed.TotalSeconds, newMessage);
						lastMessage = newMessage;
					}
				}

				while (Console.KeyAvailable)
				{
					var key = Console.ReadKey();

					if (key.KeyChar == '1') AddRequests(1);
					if (key.KeyChar == '2') AddRequests(4);
					if (key.KeyChar == '3') AddRequests(16);
					if (key.KeyChar == '4') AddRequests(64);
					if (key.KeyChar == '5') AddRequests(256);
					if (key.KeyChar == '6') AddRequests(1024);
					if (key.KeyChar == '7') AddRequests(4096);
					if (key.KeyChar == '8') AddRequests(16384);
					if (key.KeyChar == '9') AddRequests(131072);
					if (key.KeyChar == 'r') randomEnabled = !randomEnabled;
					if (key.KeyChar == 'q') return;
				}

				if (randomEnabled)
				{
					AddRequests(rand.Next(-800, 400));
				}
			}
		}

		static void AddRequests(int count)
		{
			for (int i = 0; i < count; i++)
				enqueue(ExecuteRequest);
		}

		private static TimeSpan minElapsedTime = TimeSpan.MaxValue, maxElapsedTime = TimeSpan.Zero, totalElapsedTime = TimeSpan.Zero;
		static void ExecuteRequest(object state)
		{
			try
			{
				var timer = Stopwatch.StartNew();
				execute();
				var elapsedTime = timer.Elapsed;

				lock (syncRoot)
				{
					successfulQueries++;
					if (elapsedTime < minElapsedTime) minElapsedTime = elapsedTime;
					if (elapsedTime > maxElapsedTime) maxElapsedTime = elapsedTime;
					totalElapsedTime += elapsedTime;
				}
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
				lock (syncRoot) failedQueries++;
			}
		}

		static int GetOpenConnections(int port = 5432, int? pid = null)
		{
			using (var process = new Process())
			{
				process.StartInfo = new ProcessStartInfo()
				{
					FileName = @"C:\windows\system32\netstat.exe",
					Arguments = "-n -o",
					UseShellExecute = false,
					RedirectStandardOutput = true,
					CreateNoWindow = true,
				};

				process.Start();

				var output = process.StandardOutput.ReadToEnd();

				var connections = output
					.Split('\n')
					.Skip(4)
					.Select(x => x.Trim())
					.Select(x => x.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries))
					.Where(x => x.Length >= 5)
					.Select(x => new { proto = x[0], local = x[1], remote = x[2], state = x[3], pid = int.Parse(x[4]) })
					.ToList();

				var myConnections = connections
					.Where(x => x.proto == "TCP" && (!pid.HasValue || (x.pid == pid.Value)) && x.remote.EndsWith(":" + port))
					.ToList();

				process.WaitForExit();

				return myConnections.Count;
			}
		}
	}

	public static class SimpleQuery
	{
		private static string connectionString, query;

		static SimpleQuery()
		{
			string hostname = "localhost";
			int port = 5432;
			string user = "npgsql_tests";
			string password = "npgsql_tests";
			string databaseName = "postgres";

			connectionString = string.Format(
				"Server={0};Port={1};Database={2};User Id={3};Password={4};Pooling={5};" +
				"MinPoolSize={6};MaxPoolSize={7};ConnectionLifeTime={8};Timeout={9};CommandTimeout={10};",
				hostname, port, databaseName, user, password, "true",
				0, 2, 15, 240, 240);
			query = "select 42 as result;";
		}

		public static void RunQuery()
		{
			RunQuery(connectionString, query);
		}

		private static void RunQuery(string connectionString, string query)
		{
			using (var connection = new NpgsqlConnection(connectionString))
			{
				connection.Open();

				using (var command = connection.CreateCommand())
				{
					command.CommandText = query;
					command.CommandType = CommandType.Text;

					command.Prepare();

					int result = (int)command.ExecuteScalar();
					if (result != 42)
						Console.WriteLine("ERROR!");
				}
			}
		}
	}

	// Really simple thread pool without cleanup/shutdwon/logging or anything else you may need in a production environment
	public class SimpleThreadPool
	{
		private readonly object syncRoot = new object();
		private readonly int maxSize;
		private int busyCount;
		private readonly List<Thread> threads = new List<Thread>();
		private readonly Queue<WaitCallback> requestQueue = new Queue<WaitCallback>();

		public SimpleThreadPool(int maxSize)
		{
			this.maxSize = maxSize;

			for (int i = 0; i < maxSize; i++)
			{
				AddThread();
			}
		}

		public void QueueUserWorkItem(WaitCallback callback)
		{
			lock (syncRoot)
			{
				requestQueue.Enqueue(callback);
				Monitor.Pulse(syncRoot);
			}
		}

		private void AddThread()
		{
			var thread = new Thread(Run);
			thread.Start();

			threads.Add(thread);
		}

		private void Run()
		{
			while (true)
			{
				WaitCallback callback;

				lock (syncRoot)
				{
					while (requestQueue.Count == 0)
						Monitor.Wait(syncRoot);
					callback = requestQueue.Dequeue();
					busyCount++;
				}

				try
				{
					callback(null);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex);
				}

				lock (syncRoot) busyCount--;
			}
		}

		public int GetFreeThreads()
		{
			lock (syncRoot) return maxSize - busyCount;
		}
	}

}
