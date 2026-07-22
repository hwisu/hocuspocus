import assert from "node:assert/strict";
import test from "node:test";
import {
	parseCpuTime,
	parseLinuxProcessStat,
	processStats,
} from "./process-stats.mjs";

test("parses Linux CPU ticks and resident pages", () => {
	const stat = [
		"4242 (Ktor worker (1))",
		"S 1 2 3 4 5 0 0 0 0 0",
		"123 45 0 0 20 0 4 0 999 123456 256",
	].join(" ");

	assert.deepEqual(parseLinuxProcessStat(stat, 100, 4096), {
		cpuMs: 1680,
		rssKiB: 1024,
	});
});

test("retains fractional ps CPU time for non-Linux fallback", () => {
	assert.equal(parseCpuTime("01:02.34"), 62_340);
});

test("reads statistics for the current process", () => {
	const stats = processStats(process.pid);
	assert.ok(stats.cpuMs > 0);
	assert.ok(stats.rssKiB > 0);
});
