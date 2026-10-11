import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { runtimeSecurityFinding } from "./runtime-security-policy.mjs";

const repository = path.resolve(import.meta.dirname, "..");
const lockfiles = fs.readdirSync(repository)
	.map((name) => path.join(repository, name, "gradle.lockfile"))
  .filter(fs.existsSync)
  .sort();

assert.ok(lockfiles.length > 0, "no JVM dependency lockfiles were found");

const coordinates = new Map();
for (const lockfile of lockfiles) {
  for (const line of fs.readFileSync(lockfile, "utf8").split(/\r?\n/u)) {
    const [coordinate, configurations = ""] = line.split("=", 2);
    const match = coordinate.match(/^([^:=]+):([^:=]+):([^=]+)$/u);
    // Test hosts bring their own HTTP clients; audit those transitives as well.
    const runtime = configurations.split(",").some((name) =>
      name === "runtimeClasspath" || name === "testRuntimeClasspath",
    );
    if (match === null || !runtime) continue;
    const [, group, artifact, version] = match;
    const id = `${group}:${artifact}:${version}`;
    coordinates.set(id, {
      package: {
        ecosystem: "Maven",
        name: `${group}:${artifact}`,
      },
      version,
    });
  }
}

const entries = [...coordinates.entries()].sort(([left], [right]) => left.localeCompare(right));
assert.ok(entries.length > 0, "no locked JVM runtime or test dependencies were found");

const supplementalFindings = entries.flatMap(([coordinate, query]) => {
  const id = runtimeSecurityFinding(query);
  return id === null ? [] : [{ coordinate, id }];
});

const response = await fetch("https://api.osv.dev/v1/querybatch", {
  method: "POST",
  headers: {
    "content-type": "application/json",
  },
  body: JSON.stringify({
    queries: entries.map(([, query]) => query),
  }),
});
if (!response.ok) {
  throw new Error(`OSV query failed with HTTP ${response.status}`);
}

const body = await response.json();
assert.equal(body.results.length, entries.length, "OSV returned an incomplete result set");
const findings = supplementalFindings.concat(body.results.flatMap((result, index) =>
  (result.vulns ?? []).map((vulnerability) => ({
    coordinate: entries[index][0],
    id: vulnerability.id,
  })),
));

if (findings.length > 0) {
  for (const finding of findings) {
    process.stderr.write(`${finding.coordinate}: ${finding.id}\n`);
  }
  throw new Error(`${findings.length} JVM runtime security finding(s)`);
}

process.stdout.write(
  `OSV + security floor check: ${entries.length} locked Maven runtime/test coordinates, no known findings\n`,
);
