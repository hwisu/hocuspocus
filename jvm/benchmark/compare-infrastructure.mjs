import { execFileSync, spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createConnection, createServer } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { HocuspocusProvider } from "../../packages/provider/dist/hocuspocus-provider.esm.js";
import * as Y from "../../tests/node_modules/yjs/dist/yjs.mjs";

const repoRoot = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"../..",
);
const argumentsByName = new Map(
	process.argv.slice(2).map((argument) => {
		const [name, value = "true"] = argument.replace(/^--/, "").split("=", 2);
		return [name, value];
	}),
);
const quick = argumentsByName.get("quick") === "true";
const check = argumentsByName.get("check") === "true";
const repetitions = positiveInteger(
	"repetitions",
	argumentsByName.get("repetitions") ?? (quick ? "1" : "3"),
);
const storageDocuments = positiveInteger(
	"storage-documents",
	argumentsByName.get("storage-documents") ?? (quick ? "10" : "100"),
);
const storageParallelism = positiveInteger(
	"storage-parallelism",
	argumentsByName.get("storage-parallelism") ?? (quick ? "2" : "10"),
);
const redisLatencyOperations = positiveInteger(
	"redis-latency-operations",
	argumentsByName.get("redis-latency-operations") ?? (quick ? "25" : "500"),
);
const redisBurstOperations = positiveInteger(
	"redis-burst-operations",
	argumentsByName.get("redis-burst-operations") ?? (quick ? "50" : "1_000"),
);
const output = path.resolve(
	repoRoot,
	argumentsByName.get("output") ??
		"jvm/hocuspocus-benchmark/build/reports/ab/infrastructure-latest.json",
);
const jvmExecutable =
	process.env.HOCUSPOCUS_JVM_BENCHMARK_EXECUTABLE ??
	path.join(
		repoRoot,
		"jvm/hocuspocus-benchmark/build/install/hocuspocus-benchmark/bin/hocuspocus-benchmark",
	);
const jvmOptions =
	process.env.HOCUSPOCUS_BENCHMARK_JVM_OPTS ??
	process.env.JAVA_OPTS ??
	"-Xms32m -Xmx256m -XX:MaxDirectMemorySize=128m -XX:ActiveProcessorCount=4";

const redis = await startRedis();
const rawRuns = [];
try {
	for (let repetition = 0; repetition < repetitions; repetition += 1) {
		const order = repetition % 2 === 0 ? ["node", "jvm"] : ["jvm", "node"];
		for (const target of order) {
			const sqliteRun = await runSqlite(target, repetition);
			rawRuns.push(sqliteRun);
			printRun(sqliteRun);
		}
		for (const target of order) {
			const redisRun = await runRedis(target, repetition, redis.uri);
			rawRuns.push(redisRun);
			printRun(redisRun);
		}
	}
} finally {
	await redis.close();
}

const summary = summarize(rawRuns);
const gate = evaluateGate(summary);
const report = {
	schemaVersion: 2,
	generatedAt: new Date().toISOString(),
	environment: {
		platform: process.platform,
		architecture: process.arch,
		node: process.version,
		repetitions,
		quick,
		jvmOptions,
		redisImage: redis.image,
		redisImageId: redis.imageId,
	},
	workloads: {
		sqlite: {
			documents: storageDocuments,
			parallelism: storageParallelism,
			payloadBytes: 4 * 1024,
		},
		redis: {
			recipientClients: 25,
			payloadBytes: 128,
			latencyOperations: redisLatencyOperations,
			burstOperations: redisBurstOperations,
		},
	},
	rawRuns,
	summary,
	gate,
};
await writeJson(output, report);
printSummary(summary, gate);
if (check && !gate.passed) process.exitCode = 1;

async function runSqlite(target, repetition) {
	const temporaryDirectory = await mkdtemp(
		path.join(tmpdir(), `hocuspocus-${target}-sqlite-`),
	);
	const database = path.join(temporaryDirectory, "documents.sqlite");
	const server = await startTarget(target, "sqlite", {
		HOCUSPOCUS_BENCHMARK_SQLITE_PATH: database,
	});
	try {
		await runStorageRound(server, `warmup-${target}-${repetition}`, {
			documents: quick ? 10 : 50,
			parallelism: quick ? 2 : 10,
			payloadBytes: 4 * 1024,
		});
		const before = processStats(server.child.pid);
		const baseline = await serverStats(server);
		const result = await runStorageRound(
			server,
			`sqlite-${target}-${repetition}-${Date.now()}`,
			{
				documents: storageDocuments,
				parallelism: storageParallelism,
				payloadBytes: 4 * 1024,
				baselineStores: baseline.stores,
			},
		);
		const after = processStats(server.child.pid);
		return {
			mode: "sqlite",
			target,
			repetition,
			serverCpuMs: Math.max(0, after.cpuMs - before.cpuMs),
			serverPeakRssMiB: round(Math.max(before.rssKiB, after.rssKiB) / 1024),
			...result,
		};
	} finally {
		await terminate(server.child);
		await rm(temporaryDirectory, { recursive: true, force: true });
	}
}

async function runStorageRound(
	server,
	prefix,
	{ documents, parallelism, payloadBytes, baselineStores = null },
) {
	const names = Array.from(
		{ length: documents },
		(_, index) => `${prefix}-${index}`,
	);
	const expected = payload(payloadBytes, documents);
	const initialStores = baselineStores ?? (await serverStats(server)).stores;
	const writeStarted = performance.now();
	await inBatches(names, parallelism, async (name, index) => {
		const client = await connectProvider(server.websocketUrl, name);
		try {
			client.document
				.getArray("operations")
				.push([payload(payloadBytes, index + 1)]);
			await waitUntil(
				() => !client.provider.hasUnsyncedChanges,
				15_000,
				`${server.target} SQLite acknowledgement`,
			);
		} finally {
			client.provider.destroy();
			client.document.destroy();
		}
	});
	await waitUntilAsync(
		async () => {
			const stats = await serverStats(server);
			return stats.stores >= initialStores + documents && stats.documents === 0;
		},
		30_000,
		`${server.target} SQLite stores and unloads`,
	);
	const writeMs = performance.now() - writeStarted;

	const readStarted = performance.now();
	await inBatches(names, parallelism, async (name, index) => {
		const client = await connectProvider(server.websocketUrl, name);
		try {
			const values = client.document.getArray("operations");
			const wanted = payload(payloadBytes, index + 1);
			if (values.length !== 1 || values.get(0) !== wanted) {
				throw new Error(`${server.target} SQLite reload diverged for ${name}`);
			}
		} finally {
			client.provider.destroy();
			client.document.destroy();
		}
	});
	await waitUntilAsync(
		async () => (await serverStats(server)).documents === 0,
		30_000,
		`${server.target} SQLite reload unloads`,
	);
	const readMs = performance.now() - readStarted;
	if (expected.length !== payloadBytes)
		throw new Error("payload sizing failed");
	return {
		documents,
		writeMs: round(writeMs),
		writeDocumentsPerSecond: round((documents * 1000) / writeMs),
		readMs: round(readMs),
		readDocumentsPerSecond: round((documents * 1000) / readMs),
	};
}

async function runRedis(target, repetition, redisUri) {
	const prefix = `hocuspocus-ab-${target}-${repetition}-${Date.now()}`;
	const first = await startTarget(target, "redis", {
		HOCUSPOCUS_BENCHMARK_REDIS_URI: redisUri,
		HOCUSPOCUS_BENCHMARK_REDIS_PREFIX: prefix,
		HOCUSPOCUS_BENCHMARK_IDENTIFIER: `${target}-${repetition}-a`,
	});
	const second = await startTarget(target, "redis", {
		HOCUSPOCUS_BENCHMARK_REDIS_URI: redisUri,
		HOCUSPOCUS_BENCHMARK_REDIS_PREFIX: prefix,
		HOCUSPOCUS_BENCHMARK_IDENTIFIER: `${target}-${repetition}-b`,
	});
	try {
		await runRedisRound(first, second, {
			name: `warmup-${target}-${repetition}`,
			recipientClients: quick ? 2 : 10,
			payloadBytes: 128,
			latencyOperations: quick ? 10 : 100,
			burstOperations: quick ? 20 : 200,
		});
		const before = sumProcessStats([first.child.pid, second.child.pid]);
		const result = await runRedisRound(first, second, {
			name: `redis-${target}-${repetition}-${Date.now()}`,
			recipientClients: 25,
			payloadBytes: 128,
			latencyOperations: redisLatencyOperations,
			burstOperations: redisBurstOperations,
		});
		const after = sumProcessStats([first.child.pid, second.child.pid]);
		return {
			mode: "redis",
			target,
			repetition,
			serverCpuMs: Math.max(0, after.cpuMs - before.cpuMs),
			serverPeakRssMiB: round(Math.max(before.rssKiB, after.rssKiB) / 1024),
			...result,
		};
	} finally {
		await Promise.all([terminate(first.child), terminate(second.child)]);
	}
}

async function runRedisRound(
	first,
	second,
	{ name, recipientClients, payloadBytes, latencyOperations, burstOperations },
) {
	const source = await connectProvider(first.websocketUrl, name);
	const recipients = await Promise.all(
		Array.from({ length: recipientClients }, () =>
			connectProvider(second.websocketUrl, name),
		),
	);
	const clients = [source, ...recipients];
	try {
		const arrays = clients.map((client) =>
			client.document.getArray("operations"),
		);
		const sourceArray = arrays[0];
		for (let index = 0; index < (quick ? 5 : 25); index += 1) {
			const received = waitForLength(arrays, sourceArray.length + 1, 15_000);
			sourceArray.push([payload(payloadBytes, index + 1)]);
			await received;
		}

		const latencyMs = [];
		for (let index = 0; index < latencyOperations; index += 1) {
			const received = waitForLength(arrays, sourceArray.length + 1, 15_000);
			const started = performance.now();
			sourceArray.push([payload(payloadBytes, index + 10_000)]);
			await received;
			latencyMs.push(performance.now() - started);
		}

		const expectedLength = sourceArray.length + burstOperations;
		const received = waitForLength(arrays, expectedLength, 30_000);
		const burstStarted = performance.now();
		for (let index = 0; index < burstOperations; index += 1) {
			sourceArray.push([payload(payloadBytes, index + 20_000)]);
		}
		await received;
		const burstMs = performance.now() - burstStarted;
		const expectedLast = payload(payloadBytes, 20_000 + burstOperations - 1);
		for (const array of arrays) {
			if (
				array.length !== expectedLength ||
				array.get(array.length - 1) !== expectedLast
			) {
				throw new Error(`${first.target} Redis cross-node state diverged`);
			}
		}
		return {
			recipientClients,
			latencyOperations,
			burstOperations,
			latencyP50Ms: round(percentile(latencyMs, 0.5)),
			latencyP95Ms: round(percentile(latencyMs, 0.95)),
			latencyP99Ms: round(percentile(latencyMs, 0.99)),
			burstMs: round(burstMs),
			updatesPerSecond: round((burstOperations * 1000) / burstMs),
			deliveriesPerSecond: round(
				(burstOperations * recipientClients * 1000) / burstMs,
			),
		};
	} finally {
		for (const client of clients) {
			client.provider.destroy();
			client.document.destroy();
		}
		await Promise.all([
			waitUntilAsync(
				async () => (await serverStats(first)).documents === 0,
				15_000,
				`${first.target} first Redis unload`,
			),
			waitUntilAsync(
				async () => (await serverStats(second)).documents === 0,
				15_000,
				`${second.target} second Redis unload`,
			),
		]);
	}
}

async function connectProvider(url, name) {
	const document = new Y.Doc();
	let resolveSync;
	let rejectSync;
	const synced = new Promise((resolve, reject) => {
		resolveSync = resolve;
		rejectSync = reject;
	});
	const provider = new HocuspocusProvider({
		url,
		name,
		document,
		sessionAwareness: false,
		onSynced({ state }) {
			if (state) resolveSync();
		},
		onAuthenticationFailed({ reason }) {
			rejectSync(new Error(`Authentication failed: ${reason}`));
		},
	});
	await withTimeout(synced, 30_000, `${name} connection`);
	return { document, provider };
}

async function startTarget(target, mode, extraEnvironment) {
	const port = await availablePort();
	const command = target === "node" ? process.execPath : jvmExecutable;
	const commandArguments =
		target === "node"
			? [path.join(repoRoot, "jvm/benchmark/upstream-server.mjs")]
			: [];
	const child = spawn(command, commandArguments, {
		cwd: repoRoot,
		env: {
			...process.env,
			...extraEnvironment,
			HOCUSPOCUS_BENCHMARK_PORT: String(port),
			HOCUSPOCUS_BENCHMARK_MODE: mode,
			...(target === "jvm" ? { JAVA_OPTS: jvmOptions } : {}),
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	const logs = [];
	const collectLog = (chunk) => {
		if (logs.length < 200) logs.push(chunk.toString());
	};
	child.stdout.on("data", collectLog);
	child.stderr.on("data", collectLog);
	const statsUrl = `http://127.0.0.1:${port}/benchmark/stats`;
	try {
		await waitUntilHealthy(child, statsUrl, logs);
	} catch (error) {
		await terminate(child);
		throw error;
	}
	return {
		target,
		child,
		statsUrl,
		websocketUrl:
			target === "node"
				? `ws://127.0.0.1:${port}`
				: `ws://127.0.0.1:${port}/collab`,
	};
}

async function serverStats(server) {
	const response = await fetch(server.statsUrl);
	if (!response.ok) {
		throw new Error(`${server.target} stats returned ${response.status}`);
	}
	return response.json();
}

function summarize(runs) {
	return Object.fromEntries(
		["sqlite", "redis"].map((mode) => {
			const targets = Object.fromEntries(
				["node", "jvm"].map((target) => {
					const samples = runs.filter(
						(run) => run.mode === mode && run.target === target,
					);
					const fields =
						mode === "sqlite"
							? [
									"serverCpuMs",
									"serverPeakRssMiB",
									"writeDocumentsPerSecond",
									"readDocumentsPerSecond",
								]
							: [
									"serverCpuMs",
									"serverPeakRssMiB",
									"latencyP50Ms",
									"latencyP95Ms",
									"latencyP99Ms",
									"updatesPerSecond",
									"deliveriesPerSecond",
								];
					return [
						target,
						Object.fromEntries(
							fields.map((field) => [
								field,
								median(samples.map((sample) => sample[field])),
							]),
						),
					];
				}),
			);
			const node = targets.node;
			const jvm = targets.jvm;
			const comparisons =
				mode === "sqlite"
					? {
							writeThroughputJvmToNode: ratio(
								jvm.writeDocumentsPerSecond,
								node.writeDocumentsPerSecond,
							),
							readThroughputJvmToNode: ratio(
								jvm.readDocumentsPerSecond,
								node.readDocumentsPerSecond,
							),
							cpuJvmToNode: ratio(jvm.serverCpuMs, node.serverCpuMs),
							rssJvmToNode: ratio(jvm.serverPeakRssMiB, node.serverPeakRssMiB),
						}
					: {
							latencyP95JvmToNode: ratio(jvm.latencyP95Ms, node.latencyP95Ms),
							latencyP99JvmToNode: ratio(jvm.latencyP99Ms, node.latencyP99Ms),
							throughputJvmToNode: ratio(
								jvm.updatesPerSecond,
								node.updatesPerSecond,
							),
							cpuJvmToNode: ratio(jvm.serverCpuMs, node.serverCpuMs),
							rssJvmToNode: ratio(jvm.serverPeakRssMiB, node.serverPeakRssMiB),
						};
			return [mode, { targets, comparisons }];
		}),
	);
}

function evaluateGate(summary) {
	const failures = [];
	const sqliteNode = summary.sqlite.targets.node;
	const sqliteJvm = summary.sqlite.targets.jvm;
	for (const field of ["writeDocumentsPerSecond", "readDocumentsPerSecond"]) {
		if (sqliteJvm[field] < sqliteNode[field] / 1.5) {
			failures.push(
				`sqlite ${field} ${sqliteJvm[field]} is below ${round(sqliteNode[field] / 1.5)}`,
			);
		}
	}
	const redisNode = summary.redis.targets.node;
	const redisJvm = summary.redis.targets.jvm;
	const redisP95Limit = Math.max(
		redisNode.latencyP95Ms * 1.5,
		redisNode.latencyP95Ms + 2,
	);
	const redisP99Limit = Math.max(
		redisNode.latencyP99Ms * 1.5,
		redisNode.latencyP99Ms + 5,
	);
	if (redisJvm.latencyP95Ms > redisP95Limit) {
		failures.push(
			`redis p95 ${redisJvm.latencyP95Ms}ms exceeds ${round(redisP95Limit)}ms`,
		);
	}
	if (redisJvm.latencyP99Ms > redisP99Limit) {
		failures.push(
			`redis p99 ${redisJvm.latencyP99Ms}ms exceeds ${round(redisP99Limit)}ms`,
		);
	}
	if (redisJvm.updatesPerSecond < redisNode.updatesPerSecond / 1.5) {
		failures.push(
			`redis throughput ${redisJvm.updatesPerSecond} is below ${round(redisNode.updatesPerSecond / 1.5)}`,
		);
	}
	for (const mode of ["sqlite", "redis"]) {
		const node = summary[mode].targets.node;
		const jvm = summary[mode].targets.jvm;
		if (jvm.serverPeakRssMiB > node.serverPeakRssMiB * 1.75) {
			failures.push(
				`${mode} RSS ${jvm.serverPeakRssMiB}MiB exceeds ${round(node.serverPeakRssMiB * 1.75)}MiB`,
			);
		}
	}
	return {
		passed: failures.length === 0,
		policy: {
			sqliteThroughput: "JVM >= Node / 1.5",
			redisP95: "JVM <= max(Node * 1.5, Node + 2ms)",
			redisP99: "JVM <= max(Node * 1.5, Node + 5ms)",
			redisThroughput: "JVM >= Node / 1.5",
			peakRss: "JVM <= Node * 1.75",
			cpu: "reported, not gated until process samples are long enough",
		},
		failures,
	};
}

function printRun(run) {
	if (run.mode === "sqlite") {
		console.log(
			`mode=sqlite target=${run.target} repetition=${run.repetition + 1} ` +
				`write=${run.writeDocumentsPerSecond}/s read=${run.readDocumentsPerSecond}/s ` +
				`cpuMs=${run.serverCpuMs} rssMiB=${run.serverPeakRssMiB}`,
		);
	} else {
		console.log(
			`mode=redis target=${run.target} repetition=${run.repetition + 1} ` +
				`p95=${run.latencyP95Ms}ms p99=${run.latencyP99Ms}ms ` +
				`updates=${run.updatesPerSecond}/s cpuMs=${run.serverCpuMs} ` +
				`rssMiB=${run.serverPeakRssMiB}`,
		);
	}
}

function printSummary(summary, gate) {
	console.log(
		`sqlite JVM/Node write=${summary.sqlite.comparisons.writeThroughputJvmToNode}x ` +
			`read=${summary.sqlite.comparisons.readThroughputJvmToNode}x ` +
			`cpu=${summary.sqlite.comparisons.cpuJvmToNode}x`,
	);
	console.log(
		`redis JVM/Node p95=${summary.redis.comparisons.latencyP95JvmToNode}x ` +
			`p99=${summary.redis.comparisons.latencyP99JvmToNode}x ` +
			`throughput=${summary.redis.comparisons.throughputJvmToNode}x ` +
			`cpu=${summary.redis.comparisons.cpuJvmToNode}x`,
	);
	console.log(`gate=${gate.passed ? "PASS" : "FAIL"}`);
	for (const failure of gate.failures) console.log(`  ${failure}`);
	console.log(`report=${path.relative(repoRoot, output)}`);
}

async function startRedis() {
	const externalUri = process.env.HOCUSPOCUS_BENCHMARK_REDIS_URI;
	if (externalUri) {
		await waitForTcp(new URL(externalUri), 30_000);
		return {
			uri: externalUri,
			image: "external",
			imageId: "external",
			close: async () => {},
		};
	}
	const port = await availablePort();
	const name = `hocuspocus-ab-${process.pid}-${Date.now()}`;
	const image =
		process.env.HOCUSPOCUS_BENCHMARK_REDIS_IMAGE ?? "redis:7.4-alpine";
	execFileSync(
		"docker",
		[
			"run",
			"--rm",
			"--detach",
			"--name",
			name,
			"--publish",
			`127.0.0.1:${port}:6379`,
			image,
			"redis-server",
			"--save",
			"",
			"--appendonly",
			"no",
		],
		{ stdio: "ignore" },
	);
	const uri = `redis://127.0.0.1:${port}`;
	try {
		await waitForTcp(new URL(uri), 60_000);
		const imageId = execFileSync(
			"docker",
			["image", "inspect", image, "--format", "{{.Id}}"],
			{ encoding: "utf8" },
		).trim();
		return {
			uri,
			image,
			imageId,
			close: async () => {
				try {
					execFileSync("docker", ["rm", "--force", name], {
						stdio: "ignore",
					});
				} catch {
					// The container already exited.
				}
			},
		};
	} catch (error) {
		try {
			execFileSync("docker", ["rm", "--force", name], { stdio: "ignore" });
		} catch {
			// Preserve the original startup error.
		}
		throw error;
	}
}

async function waitForTcp(url, timeoutMs) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const connected = await new Promise((resolve) => {
			const socket = createConnection({
				host: url.hostname,
				port: Number(url.port || "6379"),
			});
			socket.once("connect", () => {
				socket.destroy();
				resolve(true);
			});
			socket.once("error", () => resolve(false));
		});
		if (connected) return;
		await delay(100);
	}
	throw new Error(
		`Timed out waiting for Redis at ${url.protocol}//${url.hostname}:${url.port || "6379"}`,
	);
}

async function waitUntilHealthy(child, url, logs) {
	const deadline = Date.now() + 30_000;
	while (Date.now() < deadline) {
		if (child.exitCode !== null) {
			throw new Error(`Server exited before ready:\n${logs.join("")}`);
		}
		try {
			const response = await fetch(url);
			if (response.ok) return;
		} catch {
			// Server is still starting.
		}
		await delay(50);
	}
	throw new Error(`Timed out waiting for ${url}:\n${logs.join("")}`);
}

function waitForLength(arrays, expectedLength, timeoutMs) {
	if (arrays.every((array) => array.length === expectedLength)) {
		return Promise.resolve();
	}
	return new Promise((resolve, reject) => {
		let settled = false;
		const observer = () => {
			if (
				!settled &&
				arrays.every((array) => array.length === expectedLength)
			) {
				settled = true;
				cleanup();
				resolve();
			}
		};
		const timer = setTimeout(() => {
			if (settled) return;
			settled = true;
			cleanup();
			reject(
				new Error(`Timed out waiting for document length ${expectedLength}`),
			);
		}, timeoutMs);
		const cleanup = () => {
			clearTimeout(timer);
			for (const array of arrays) array.unobserve(observer);
		};
		for (const array of arrays) array.observe(observer);
		observer();
	});
}

async function inBatches(values, parallelism, operation) {
	for (let index = 0; index < values.length; index += parallelism) {
		const batch = values.slice(index, index + parallelism);
		await Promise.all(
			batch.map((value, offset) => operation(value, index + offset)),
		);
	}
}

async function waitUntil(predicate, timeoutMs, label) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (predicate()) return;
		await delay(1);
	}
	throw new Error(`Timed out waiting for ${label}`);
}

async function waitUntilAsync(predicate, timeoutMs, label) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await delay(10);
	}
	throw new Error(`Timed out waiting for ${label}`);
}

function processStats(pid) {
	try {
		const output = execFileSync(
			"/bin/ps",
			["-o", "time=", "-o", "rss=", "-p", String(pid)],
			{ encoding: "utf8" },
		).trim();
		const fields = output.split(/\s+/);
		if (fields.length !== 2) return { cpuMs: 0, rssKiB: 0 };
		return { cpuMs: parseCpuTime(fields[0]), rssKiB: Number(fields[1]) };
	} catch {
		return { cpuMs: 0, rssKiB: 0 };
	}
}

function sumProcessStats(pids) {
	return pids.map(processStats).reduce(
		(total, current) => ({
			cpuMs: total.cpuMs + current.cpuMs,
			rssKiB: total.rssKiB + current.rssKiB,
		}),
		{ cpuMs: 0, rssKiB: 0 },
	);
}

function parseCpuTime(value) {
	const [clock, fraction = ""] = value.split(".");
	const parts = clock.split(":").map(Number);
	let seconds = 0;
	for (const part of parts) seconds = seconds * 60 + part;
	return seconds * 1000 + Number(`0.${fraction || "0"}`) * 1000;
}

async function availablePort() {
	return new Promise((resolve, reject) => {
		const server = createServer();
		server.once("error", reject);
		server.listen(0, "127.0.0.1", () => {
			const address = server.address();
			const port = typeof address === "object" && address ? address.port : null;
			server.close((error) => {
				if (error) reject(error);
				else if (port === null) reject(new Error("Failed to allocate a port"));
				else resolve(port);
			});
		});
	});
}

async function terminate(child) {
	if (child.exitCode !== null) return;
	await new Promise((resolve) => {
		const timeout = setTimeout(() => {
			if (child.exitCode === null) child.kill("SIGKILL");
		}, 5_000);
		child.once("exit", () => {
			clearTimeout(timeout);
			resolve();
		});
		child.kill("SIGTERM");
	});
}

function withTimeout(promise, timeoutMs, label) {
	return new Promise((resolve, reject) => {
		const timeout = setTimeout(
			() => reject(new Error(`Timed out waiting for ${label}`)),
			timeoutMs,
		);
		promise.then(
			(value) => {
				clearTimeout(timeout);
				resolve(value);
			},
			(error) => {
				clearTimeout(timeout);
				reject(error);
			},
		);
	});
}

function payload(size, sequence) {
	const prefix = `${sequence.toString(16).padStart(8, "0")}:`;
	return prefix + "x".repeat(Math.max(0, size - prefix.length));
}

function percentile(values, fraction) {
	const ordered = [...values].sort((left, right) => left - right);
	const index = Math.min(
		ordered.length - 1,
		Math.ceil(ordered.length * fraction) - 1,
	);
	return ordered[Math.max(0, index)];
}

function median(values) {
	const ordered = [...values].sort((left, right) => left - right);
	const middle = Math.floor(ordered.length / 2);
	const value =
		ordered.length % 2 === 0
			? (ordered[middle - 1] + ordered[middle]) / 2
			: ordered[middle];
	return round(value);
}

function ratio(left, right) {
	if (right === 0) return left === 0 ? 1 : null;
	return round(left / right);
}

function round(value) {
	return Number(value.toFixed(3));
}

function positiveInteger(name, value) {
	const parsed = Number(String(value).replaceAll("_", ""));
	if (!Number.isInteger(parsed) || parsed <= 0) {
		throw new Error(`--${name} must be a positive integer`);
	}
	return parsed;
}

async function writeJson(file, value) {
	await mkdir(path.dirname(file), { recursive: true });
	await writeFile(file, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

function delay(milliseconds) {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
