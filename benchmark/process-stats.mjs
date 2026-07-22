import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import process from "node:process";

const linuxClockTicksPerSecond =
	process.platform === "linux" ? readSystemConstant("CLK_TCK") : null;
const linuxPageSizeBytes =
	process.platform === "linux" ? readSystemConstant("PAGESIZE") : null;

export function processStats(pid) {
	if (linuxClockTicksPerSecond !== null && linuxPageSizeBytes !== null) {
		try {
			return parseLinuxProcessStat(
				readFileSync(`/proc/${pid}/stat`, "utf8"),
				linuxClockTicksPerSecond,
				linuxPageSizeBytes,
			);
		} catch {
			// Fall through to ps when procfs is unavailable or the process exits.
		}
	}

	try {
		const output = execFileSync(
			"/bin/ps",
			["-o", "time=", "-o", "rss=", "-p", String(pid)],
			{ encoding: "utf8" },
		).trim();
		const fields = output.split(/\s+/);
		if (fields.length !== 2) return emptyStats();
		return { cpuMs: parseCpuTime(fields[0]), rssKiB: Number(fields[1]) };
	} catch {
		return emptyStats();
	}
}

export function parseLinuxProcessStat(
	value,
	clockTicksPerSecond,
	pageSizeBytes,
) {
	const commandEnd = value.lastIndexOf(")");
	if (
		commandEnd < 0 ||
		!Number.isFinite(clockTicksPerSecond) ||
		clockTicksPerSecond <= 0 ||
		!Number.isFinite(pageSizeBytes) ||
		pageSizeBytes <= 0
	) {
		throw new Error("Invalid Linux process statistics");
	}

	// Fields after comm start at field 3 (state). See proc_pid_stat(5).
	const fields = value
		.slice(commandEnd + 1)
		.trim()
		.split(/\s+/);
	const userTicks = Number(fields[11]);
	const systemTicks = Number(fields[12]);
	const residentPages = Number(fields[21]);
	if (
		fields.length < 22 ||
		![userTicks, systemTicks, residentPages].every(Number.isFinite)
	) {
		throw new Error("Incomplete Linux process statistics");
	}

	return {
		cpuMs: ((userTicks + systemTicks) * 1000) / clockTicksPerSecond,
		rssKiB: (residentPages * pageSizeBytes) / 1024,
	};
}

export function parseCpuTime(value) {
	const [clock, fraction = ""] = value.split(".");
	const parts = clock.split(":").map(Number);
	let seconds = 0;
	for (const part of parts) seconds = seconds * 60 + part;
	return seconds * 1000 + Number(`0.${fraction || "0"}`) * 1000;
}

function readSystemConstant(name) {
	try {
		const value = Number(
			execFileSync("getconf", [name], { encoding: "utf8" }).trim(),
		);
		return Number.isFinite(value) && value > 0 ? value : null;
	} catch {
		return null;
	}
}

function emptyStats() {
	return { cpuMs: 0, rssKiB: 0 };
}
