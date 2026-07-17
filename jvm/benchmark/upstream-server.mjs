import { Redis } from "../../packages/extension-redis/dist/hocuspocus-redis.esm.js";
import { SQLite } from "../../packages/extension-sqlite/dist/hocuspocus-sqlite.esm.js";
import { Server } from "../../packages/server/dist/hocuspocus-server.esm.js";

const port = Number(process.env.HOCUSPOCUS_BENCHMARK_PORT ?? "19876");
const mode = process.env.HOCUSPOCUS_BENCHMARK_MODE ?? "core";
let stores = 0;
const extensions = [];

if (mode === "sqlite") {
	const database = process.env.HOCUSPOCUS_BENCHMARK_SQLITE_PATH;
	if (!database) {
		throw new Error(
			"HOCUSPOCUS_BENCHMARK_SQLITE_PATH is required in sqlite mode",
		);
	}
	extensions.push(new SQLite({ database }));
	extensions.push({
		priority: -100,
		afterStoreDocument() {
			stores += 1;
		},
	});
} else if (mode === "redis") {
	const redisUri = process.env.HOCUSPOCUS_BENCHMARK_REDIS_URI;
	const identifier = process.env.HOCUSPOCUS_BENCHMARK_IDENTIFIER;
	if (!redisUri || !identifier) {
		throw new Error(
			"HOCUSPOCUS_BENCHMARK_REDIS_URI and HOCUSPOCUS_BENCHMARK_IDENTIFIER are required in redis mode",
		);
	}
	const parsedRedisUri = new URL(redisUri);
	if (!["redis:", "rediss:"].includes(parsedRedisUri.protocol)) {
		throw new Error("HOCUSPOCUS_BENCHMARK_REDIS_URI must use redis or rediss");
	}
	const database = parsedRedisUri.pathname.replace(/^\//, "");
	if (
		database &&
		(!/^\d+$/.test(database) || !Number.isSafeInteger(Number(database)))
	) {
		throw new Error("Redis database must be a non-negative integer");
	}
	extensions.push(
		new Redis({
			host: parsedRedisUri.hostname,
			port: Number(parsedRedisUri.port || "6379"),
			options: {
				username: parsedRedisUri.username
					? decodeURIComponent(parsedRedisUri.username)
					: undefined,
				password: parsedRedisUri.password
					? decodeURIComponent(parsedRedisUri.password)
					: undefined,
				db: database ? Number(database) : undefined,
				tls: parsedRedisUri.protocol === "rediss:" ? {} : undefined,
			},
			identifier,
			prefix:
				process.env.HOCUSPOCUS_BENCHMARK_REDIS_PREFIX ?? "hocuspocus-benchmark",
			disconnectDelay: 0,
			awaitInitialSyncTimeout: 2_000,
		}),
	);
} else if (mode !== "core") {
	throw new Error(`Unsupported HOCUSPOCUS_BENCHMARK_MODE: ${mode}`);
}

extensions.push({
	priority: -1_000,
	onRequest({ request, response, instance }) {
		if (request.url !== "/benchmark/stats") return;
		response.writeHead(200, { "Content-Type": "application/json" });
		response.end(
			JSON.stringify({
				mode,
				stores,
				documents: instance.documents.size,
			}),
		);
		throw null;
	},
});

const server = new Server({
	address: "127.0.0.1",
	port,
	quiet: true,
	stopOnSignals: false,
	extensions,
	debounce: mode === "sqlite" ? 0 : 2_000,
	maxDebounce: mode === "sqlite" ? 0 : 10_000,
});

await server.listen();
process.stdout.write(`${JSON.stringify({ ready: true, port })}\n`);

let stopping = false;
const stop = async () => {
	if (stopping) return;
	stopping = true;
	server.hocuspocus.closeConnections();
	await server.destroy();
};

for (const signal of ["SIGINT", "SIGTERM"]) {
	process.on(signal, () => {
		stop()
			.then(() => process.exit(0))
			.catch((error) => {
				console.error(error);
				process.exit(1);
			});
	});
}
