import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import process from "node:process";
import test from "node:test";
import { runtimeSecurityFinding } from "./runtime-security-policy.mjs";

function finding(name, version) {
	return runtimeSecurityFinding({ package: { ecosystem: "Maven", name }, version });
}

test("rejects vulnerable Netty versions even when OSV has no findings", () => {
	for (const version of ["4.1.137.Final", "4.1.138.Final", "4.2.17.Final", "4.2.18.Final"]) {
		assert.match(finding("io.netty:netty-codec-http", version), /Netty security release/u);
	}
});

test("accepts patched Netty versions on both supported release lines", () => {
	for (const version of ["4.1.139.Final", "4.1.140.Final", "4.2.19.Final", "4.2.20.Final", "4.2.100.Final"]) {
		assert.equal(finding("io.netty:netty-handler", version), null);
	}
});

test("rejects Logback versions before the complete MDC sanitization fix", () => {
	for (const name of ["ch.qos.logback:logback-classic", "ch.qos.logback:logback-core"]) {
		for (const version of ["1.5.38", "1.6.3", "1.6.4"]) {
			assert.match(finding(name, version), /CVE-2026-104721/u);
		}
		for (const version of ["1.6.5", "1.6.10", "1.7.0"]) {
			assert.equal(finding(name, version), null);
		}
	}
});

test("leaves unrelated packages and independently versioned Netty artifacts to OSV", () => {
	assert.equal(finding("org.slf4j:slf4j-api", "2.0.18"), null);
	assert.equal(finding("io.netty:netty-tcnative-boringssl-static", "2.0.77.Final"), null);
	assert.equal(runtimeSecurityFinding({
		package: { ecosystem: "npm", name: "io.netty:netty-handler" },
		version: "4.2.17.Final",
	}), null);
});

function runAudit(t, lockfile, vulnerability = null) {
	const repository = fs.mkdtempSync(path.join(os.tmpdir(), "hocuspocus-security-"));
	t.after(() => fs.rmSync(repository, { recursive: true, force: true }));
	fs.mkdirSync(path.join(repository, "scripts"));
	fs.mkdirSync(path.join(repository, "module"));
	fs.writeFileSync(path.join(repository, "module", "gradle.lockfile"), lockfile);
	for (const name of ["check-runtime-vulnerabilities.mjs", "runtime-security-policy.mjs"]) {
		fs.copyFileSync(path.join(import.meta.dirname, name), path.join(repository, "scripts", name));
	}
	const mock = path.join(repository, "mock-osv.mjs");
	fs.writeFileSync(mock, `
globalThis.fetch = async (_url, options) => ({
  ok: true,
  json: async () => ({ results: JSON.parse(options.body).queries.map(query => ({
    vulns: query.package.name === "org.apache.httpcomponents.client5:httpclient5"
      ? ${JSON.stringify(vulnerability ? [{ id: vulnerability }] : [])} : [],
  })) }),
});
`);
	const result = spawnSync(process.execPath, ["--import", mock, path.join(repository, "scripts", "check-runtime-vulnerabilities.mjs")], {
		encoding: "utf8",
		timeout: 10_000,
	});
	assert.ifError(result.error);
	return result;
}

test("the audit fails on a vulnerable test-only dependency", (t) => {
	const result = runAudit(t,
		"org.slf4j:slf4j-api:2.0.19=runtimeClasspath\norg.apache.httpcomponents.client5:httpclient5:5.5.1=testRuntimeClasspath\n",
		"GHSA-hjcp-jmpx-g3qm",
	);
	assert.equal(result.status, 1);
	assert.match(result.stderr, /httpclient5:5\.5\.1: GHSA-hjcp-jmpx-g3qm/u);
});

test("the audit enforces release security floors when OSV returns no findings", (t) => {
	const result = runAudit(t, "io.netty:netty-handler:4.2.18.Final=runtimeClasspath\n");
	assert.equal(result.status, 1);
	assert.match(result.stderr, /requires >= 4\.2\.19\.Final/u);
});

test("the audit succeeds with patched runtime and test dependencies", (t) => {
	const result = runAudit(t,
		"io.netty:netty-handler:4.2.19.Final=runtimeClasspath\norg.apache.httpcomponents.client5:httpclient5:5.6.4=testRuntimeClasspath\n",
	);
	assert.equal(result.status, 0, result.stderr);
	assert.match(result.stdout, /2 locked Maven runtime\/test coordinates, no known findings/u);
});
