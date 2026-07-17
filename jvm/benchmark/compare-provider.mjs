import { execFileSync, spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
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
const repetitions = Number(
	argumentsByName.get("repetitions") ?? (quick ? "1" : "3"),
);
const latencyScale = Number(argumentsByName.get("latency-scale") ?? "1");
const output = path.resolve(
	repoRoot,
	argumentsByName.get("output") ??
		"jvm/hocuspocus-benchmark/build/reports/ab/latest.json",
);
const jvmProfile = argumentsByName.has("profile-jvm")
	? path.resolve(repoRoot, argumentsByName.get("profile-jvm"))
	: null;
const jvmExecutable =
	process.env.HOCUSPOCUS_JVM_BENCHMARK_EXECUTABLE ??
	path.join(
		repoRoot,
		"jvm/hocuspocus-benchmark/build/install/hocuspocus-benchmark/bin/hocuspocus-benchmark",
	);

if (
	!Number.isInteger(repetitions) ||
	repetitions <= 0 ||
	!Number.isFinite(latencyScale) ||
	latencyScale <= 0
) {
	throw new Error("--repetitions and --latency-scale must be positive");
}

const scale = quick ? 0.25 : 1;
const scenarios = [
	{
		name: "small-10",
		clients: 10,
		payloadBytes: 128,
		warmupOperations: scaled(12),
		latencyOperations: scaled(100 * latencyScale),
		burstOperations: scaled(200),
	},
	{
		name: "small-100",
		clients: 100,
		payloadBytes: 128,
		warmupOperations: scaled(8),
		latencyOperations: scaled(50 * latencyScale),
		burstOperations: scaled(150),
	},
	{
		name: "large-25",
		clients: 25,
		payloadBytes: 16 * 1024,
		warmupOperations: scaled(5),
		latencyOperations: scaled(25 * latencyScale),
		burstOperations: scaled(50),
	},
];

const rawRuns = [];
for (let repetition = 0; repetition < repetitions; repetition += 1) {
	const order = repetition % 2 === 0 ? ["node", "jvm"] : ["jvm", "node"];
	for (const target of order) {
		const run = await runTarget(target, repetition);
		rawRuns.push(run);
		printRun(run);
	}
}

const summary = summarize(rawRuns);
const gate = evaluateGate(summary);
const report = {
	schemaVersion: 1,
	generatedAt: new Date().toISOString(),
	environment: {
		platform: process.platform,
		architecture: process.arch,
		node: process.version,
		repetitions,
		quick,
		latencyScale,
	},
	scenarios,
	rawRuns,
	summary,
	gate,
};

await mkdir(path.dirname(output), { recursive: true });
await writeFile(output, `${JSON.stringify(report, null, 2)}\n`, "utf8");
printSummary(summary);
console.log(`gate=${gate.passed ? "PASS" : "FAIL"}`);
for (const failure of gate.failures) console.log(`  ${failure}`);
console.log(`report=${path.relative(repoRoot, output)}`);
if (check && !gate.passed) process.exitCode = 1;

function scaled(value) {
	return Math.max(5, Math.round(value * scale));
}

async function runTarget(target, repetition) {
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
			HOCUSPOCUS_BENCHMARK_PORT: String(port),
		},
		stdio: ["ignore", "pipe", "pipe"],
	});
	const logs = [];
	const collectLog = (chunk) => {
		if (logs.length < 200) logs.push(chunk.toString());
	};
	child.stdout.on("data", collectLog);
	child.stderr.on("data", collectLog);

	const healthUrl =
		target === "node"
			? `http://127.0.0.1:${port}/`
			: `http://127.0.0.1:${port}/health`;
	const websocketUrl =
		target === "node"
			? `ws://127.0.0.1:${port}`
			: `ws://127.0.0.1:${port}/collab`;

	try {
		await waitUntilHealthy(child, healthUrl, logs);
		await runScenario(websocketUrl, target, repetition, {
			name: "server-warmup",
			clients: 10,
			payloadBytes: 128,
			warmupOperations: scaled(400),
			latencyOperations: scaled(100),
			burstOperations: scaled(300),
		});
		if (target === "jvm" && jvmProfile !== null) {
			startJfr(child.pid, jvmProfile);
		}
		const start = processStats(child.pid);
		let peakRssKiB = start.rssKiB;
		const sampler = setInterval(() => {
			const current = processStats(child.pid);
			peakRssKiB = Math.max(peakRssKiB, current.rssKiB);
		}, 25);

		const scenarioResults = [];
		try {
			for (const scenario of scenarios) {
				const scenarioStart = processStats(child.pid);
				const result = await runScenario(
					websocketUrl,
					target,
					repetition,
					scenario,
					() => processStats(child.pid),
				);
				const scenarioEnd = processStats(child.pid);
				scenarioResults.push({
					...result,
					serverLifecycleCpuMs: Math.max(
						0,
						scenarioEnd.cpuMs - scenarioStart.cpuMs,
					),
				});
			}
		} finally {
			clearInterval(sampler);
		}

		const end = processStats(child.pid);
		if (target === "jvm" && jvmProfile !== null) {
			stopJfr(child.pid);
		}
		peakRssKiB = Math.max(peakRssKiB, end.rssKiB);
		return {
			target,
			repetition,
			serverCpuMs: Math.max(0, end.cpuMs - start.cpuMs),
			serverBaseRssMiB: round(start.rssKiB / 1024),
			serverPeakRssMiB: round(peakRssKiB / 1024),
			scenarios: scenarioResults,
		};
	} finally {
		await terminate(child);
	}
}

async function runScenario(
	url,
	target,
	repetition,
	scenario,
	readServerStats = null,
) {
	const documentName = [
		"ab",
		target,
		repetition,
		scenario.name,
		Date.now(),
		Math.random().toString(16).slice(2),
	].join("-");
	const clients = [];
	const syncPromises = [];
	const connectStart = performance.now();

	for (let index = 0; index < scenario.clients; index += 1) {
		const document = new Y.Doc();
		let resolveSync;
		let rejectSync;
		const synced = new Promise((resolve, reject) => {
			resolveSync = resolve;
			rejectSync = reject;
		});
		const provider = new HocuspocusProvider({
			url,
			name: documentName,
			document,
			sessionAwareness: false,
			onSynced({ state }) {
				if (state) resolveSync();
			},
			onAuthenticationFailed({ reason }) {
				rejectSync(new Error(`Authentication failed: ${reason}`));
			},
		});
		clients.push({ document, provider });
		syncPromises.push(synced);
	}

	try {
		await withTimeout(
			Promise.all(syncPromises),
			30_000,
			`${scenario.name} connection`,
		);
		const connectMs = performance.now() - connectStart;
		const arrays = clients.map((client) =>
			client.document.getArray("operations"),
		);
		const source = arrays[0];
		let sequence = 0;

		for (
			let operation = 0;
			operation < scenario.warmupOperations;
			operation += 1
		) {
			sequence += 1;
			const expected = source.length + 1;
			const received = waitForLength(arrays, expected, 10_000);
			source.push([payload(scenario.payloadBytes, sequence)]);
			await received;
		}

		const workloadCpuStart = readServerStats?.().cpuMs ?? 0;
		const latencyMs = [];
		for (
			let operation = 0;
			operation < scenario.latencyOperations;
			operation += 1
		) {
			sequence += 1;
			const expected = source.length + 1;
			const received = waitForLength(arrays, expected, 10_000);
			const startedAt = performance.now();
			source.push([payload(scenario.payloadBytes, sequence)]);
			await received;
			latencyMs.push(performance.now() - startedAt);
		}

		const burstExpectedLength = source.length + scenario.burstOperations;
		const burstReceived = waitForLength(arrays, burstExpectedLength, 30_000);
		const burstStartedAt = performance.now();
		for (
			let operation = 0;
			operation < scenario.burstOperations;
			operation += 1
		) {
			sequence += 1;
			source.push([payload(scenario.payloadBytes, sequence)]);
		}
		await burstReceived;
		const burstMs = performance.now() - burstStartedAt;
		const workloadCpuEnd = readServerStats?.().cpuMs ?? 0;

		const expectedLastValue = payload(scenario.payloadBytes, sequence);
		for (const array of arrays) {
			if (
				array.length !== burstExpectedLength ||
				array.get(array.length - 1) !== expectedLastValue
			) {
				throw new Error(
					`${target}/${scenario.name} converged to different document state`,
				);
			}
		}

		return {
			name: scenario.name,
			clients: scenario.clients,
			payloadBytes: scenario.payloadBytes,
			connectMs: round(connectMs),
			latencyP50Ms: round(percentile(latencyMs, 0.5)),
			latencyP95Ms: round(percentile(latencyMs, 0.95)),
			latencyP99Ms: round(percentile(latencyMs, 0.99)),
			burstOperations: scenario.burstOperations,
			burstMs: round(burstMs),
			updatesPerSecond: round((scenario.burstOperations * 1000) / burstMs),
			fanoutDeliveriesPerSecond: round(
				(scenario.burstOperations * (scenario.clients - 1) * 1000) / burstMs,
			),
			serverWorkloadCpuMs: Math.max(0, workloadCpuEnd - workloadCpuStart),
		};
	} finally {
		for (const client of clients) {
			client.provider.destroy();
			client.document.destroy();
		}
		await delay(25);
	}
}

function waitForLength(arrays, expectedLength, timeoutMs) {
	if (arrays.every((array) => array.length === expectedLength))
		return Promise.resolve();
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

function summarize(runs) {
	const targets = {};
	for (const target of ["node", "jvm"]) {
		const targetRuns = runs.filter((run) => run.target === target);
		targets[target] = {
			serverCpuMs: median(targetRuns.map((run) => run.serverCpuMs)),
			serverBaseRssMiB: median(targetRuns.map((run) => run.serverBaseRssMiB)),
			serverPeakRssMiB: median(targetRuns.map((run) => run.serverPeakRssMiB)),
			scenarios: Object.fromEntries(
				scenarios.map((scenario) => {
					const samples = targetRuns.map((run) =>
						run.scenarios.find((value) => value.name === scenario.name),
					);
					return [
						scenario.name,
						{
							connectMs: median(samples.map((sample) => sample.connectMs)),
							latencyP50Ms: median(
								samples.map((sample) => sample.latencyP50Ms),
							),
							latencyP95Ms: median(
								samples.map((sample) => sample.latencyP95Ms),
							),
							latencyP99Ms: median(
								samples.map((sample) => sample.latencyP99Ms),
							),
							updatesPerSecond: median(
								samples.map((sample) => sample.updatesPerSecond),
							),
							fanoutDeliveriesPerSecond: median(
								samples.map((sample) => sample.fanoutDeliveriesPerSecond),
							),
							serverWorkloadCpuMs: median(
								samples.map((sample) => sample.serverWorkloadCpuMs),
							),
							serverLifecycleCpuMs: median(
								samples.map((sample) => sample.serverLifecycleCpuMs),
							),
						},
					];
				}),
			),
		};
	}

	const comparisons = Object.fromEntries(
		scenarios.map((scenario) => {
			const node = targets.node.scenarios[scenario.name];
			const jvm = targets.jvm.scenarios[scenario.name];
			return [
				scenario.name,
				{
					connectJvmToNode: ratio(jvm.connectMs, node.connectMs),
					latencyP95JvmToNode: ratio(jvm.latencyP95Ms, node.latencyP95Ms),
					latencyP99JvmToNode: ratio(jvm.latencyP99Ms, node.latencyP99Ms),
					throughputJvmToNode: ratio(
						jvm.updatesPerSecond,
						node.updatesPerSecond,
					),
					serverWorkloadCpuJvmToNode: ratio(
						jvm.serverWorkloadCpuMs,
						node.serverWorkloadCpuMs,
					),
					serverLifecycleCpuJvmToNode: ratio(
						jvm.serverLifecycleCpuMs,
						node.serverLifecycleCpuMs,
					),
				},
			];
		}),
	);
	return { targets, comparisons };
}

function printRun(run) {
	console.log(
		`target=${run.target} repetition=${run.repetition + 1} ` +
			`cpuMs=${run.serverCpuMs} peakRssMiB=${run.serverPeakRssMiB}`,
	);
	for (const scenario of run.scenarios) {
		console.log(
			`  ${scenario.name} connectMs=${scenario.connectMs} ` +
				`p50=${scenario.latencyP50Ms} p95=${scenario.latencyP95Ms} ` +
				`p99=${scenario.latencyP99Ms} updatesPerSecond=${scenario.updatesPerSecond}`,
		);
	}
}

function printSummary(summary) {
	console.log(
		"median comparison (latency ratio JVM/Node, throughput ratio JVM/Node)",
	);
	for (const scenario of scenarios) {
		const comparison = summary.comparisons[scenario.name];
		console.log(
			`  ${scenario.name} p95=${comparison.latencyP95JvmToNode}x ` +
				`p99=${comparison.latencyP99JvmToNode}x ` +
				`throughput=${comparison.throughputJvmToNode}x ` +
				`workloadCpu=${comparison.serverWorkloadCpuJvmToNode}x`,
		);
	}
	console.log(
		`server peak RSS median Node=${summary.targets.node.serverPeakRssMiB} MiB ` +
			`JVM=${summary.targets.jvm.serverPeakRssMiB} MiB`,
	);
}

function evaluateGate(summary) {
	const failures = [];
	for (const scenario of scenarios) {
		const node = summary.targets.node.scenarios[scenario.name];
		const jvm = summary.targets.jvm.scenarios[scenario.name];
		const p95Limit = Math.max(node.latencyP95Ms * 1.5, node.latencyP95Ms + 1);
		const p99Limit = Math.max(node.latencyP99Ms * 1.5, node.latencyP99Ms + 3);
		const throughputMinimum = node.updatesPerSecond / 1.5;
		const cpuLimit = Math.max(
			node.serverWorkloadCpuMs * 1.5,
			node.serverWorkloadCpuMs + 50,
		);
		if (jvm.latencyP95Ms > p95Limit) {
			failures.push(
				`${scenario.name}: p95 ${jvm.latencyP95Ms}ms exceeds ${round(p95Limit)}ms`,
			);
		}
		if (jvm.latencyP99Ms > p99Limit) {
			failures.push(
				`${scenario.name}: p99 ${jvm.latencyP99Ms}ms exceeds ${round(p99Limit)}ms`,
			);
		}
		if (jvm.updatesPerSecond < throughputMinimum) {
			failures.push(
				`${scenario.name}: throughput ${jvm.updatesPerSecond}/s is below ` +
					`${round(throughputMinimum)}/s`,
			);
		}
		if (jvm.serverWorkloadCpuMs > cpuLimit) {
			failures.push(
				`${scenario.name}: workload CPU ${jvm.serverWorkloadCpuMs}ms exceeds ` +
					`${round(cpuLimit)}ms`,
			);
		}
	}
	const rssLimit = summary.targets.node.serverPeakRssMiB * 1.75;
	if (summary.targets.jvm.serverPeakRssMiB > rssLimit) {
		failures.push(
			`peak RSS ${summary.targets.jvm.serverPeakRssMiB}MiB exceeds ${round(rssLimit)}MiB`,
		);
	}
	return {
		passed: failures.length === 0,
		policy: {
			p95: "JVM <= max(Node * 1.5, Node + 1ms)",
			p99: "JVM <= max(Node * 1.5, Node + 3ms)",
			throughput: "JVM >= Node / 1.5",
			workloadCpu: "JVM <= max(Node * 1.5, Node + 50ms)",
			peakRss: "JVM <= Node * 1.75",
		},
		failures,
	};
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

function startJfr(pid, recording) {
	const jcmd = path.join(
		process.env.JAVA_HOME ??
			"/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
		"bin/jcmd",
	);
	execFileSync(
		jcmd,
		[
			String(pid),
			"JFR.start",
			"name=hocuspocus-ab",
			"settings=profile",
			`filename=${recording}`,
		],
		{ stdio: "ignore" },
	);
}

function stopJfr(pid) {
	const jcmd = path.join(
		process.env.JAVA_HOME ??
			"/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
		"bin/jcmd",
	);
	execFileSync(jcmd, [String(pid), "JFR.stop", "name=hocuspocus-ab"], {
		stdio: "ignore",
	});
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

function delay(milliseconds) {
	return new Promise((resolve) => setTimeout(resolve, milliseconds));
}
