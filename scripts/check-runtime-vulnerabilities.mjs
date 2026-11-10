import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";
import { runtimeSecurityFinding } from "./runtime-security-policy.mjs";

const repository = path.resolve(import.meta.dirname, "..");

export function collectMavenDependencies(root) {
  const lockfiles = fs.readdirSync(root)
    .map((name) => path.join(root, name, "gradle.lockfile"))
    .filter(fs.existsSync)
    .sort();
  assert.ok(lockfiles.length > 0, "no JVM dependency lockfiles were found");

  const coordinates = new Map();
  function add(group, artifact, version) {
    coordinates.set(`${group}:${artifact}:${version}`, {
      package: { ecosystem: "Maven", name: `${group}:${artifact}` },
      version,
    });
  }
  for (const lockfile of lockfiles) {
    for (const line of fs.readFileSync(lockfile, "utf8").split(/\r?\n/u)) {
      const match = line.match(/^([^:#=]+):([^:=]+):([^=]+)=/u);
      if (match !== null) add(...match.slice(1));
    }
  }
  assert.ok(coordinates.size > 0, "no locked JVM dependencies were found");

  // Gradle plugins are not part of the runtime lockfile. Include both builds'
  // verified binaries so the consumer/compiler checks cannot hide advisories.
  for (const relative of [
    "gradle/verification-metadata.xml",
    "consumer-smoke/gradle/verification-metadata.xml",
  ]) {
    const metadata = fs.readFileSync(path.join(root, relative), "utf8");
    const components = [...metadata.matchAll(/<component group="([^"]+)" name="([^"]+)" version="([^"]+)">([\s\S]*?)<\/component>/gu)];
    assert.ok(components.length > 0, `no verified Maven components found in ${relative}`);
    for (const match of components) {
      // Parent POMs and overridden version descriptors are read during resolution,
      // but their binaries are not loaded. Locked coordinates are audited above.
      if (/<artifact name="[^"]+\.jar"/u.test(match[4])) add(...match.slice(1, 4));
    }
  }
  const wrapper = fs.readFileSync(path.join(root, "gradle/wrapper/gradle-wrapper.properties"), "utf8");
  const gradleVersion = wrapper.match(/^distributionUrl=.*\/gradle-([\d.]+)-(?:bin|all)\.zip$/mu)?.[1];
  assert.ok(gradleVersion, "the Gradle wrapper version is missing");
  add("org.gradle", "gradle-core", gradleVersion);
  return [...coordinates.entries()].sort(([left], [right]) => left.localeCompare(right));
}

export async function checkMavenDependencies(entries, fetchImplementation = fetch) {
  const findings = entries.flatMap(([coordinate, query]) => {
    const id = runtimeSecurityFinding(query);
    return id === null ? [] : [{ coordinate, id }];
  });
  let pending = entries;
  // OSV may split one package's advisories across pages, and a page can hold
  // a continuation token without any advisories, so every token must be followed.
  while (pending.length > 0) {
    const response = await fetchImplementation("https://api.osv.dev/v1/querybatch", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ queries: pending.map(([, query]) => query) }),
      signal: AbortSignal.timeout(60_000),
    });
    if (!response.ok) {
      throw new Error(`OSV query failed with HTTP ${response.status}`);
    }

    const body = await response.json();
    assert.equal(body.results.length, pending.length, "OSV returned an incomplete result set");
    const nextPages = [];
    for (const [index, result] of body.results.entries()) {
      const [coordinate, query] = pending[index];
      for (const vulnerability of result.vulns ?? []) {
        findings.push({ coordinate, id: vulnerability.id });
      }
      if (result.next_page_token) {
        nextPages.push([coordinate, { ...query, page_token: result.next_page_token }]);
      }
    }
    pending = nextPages;
  }

  if (findings.length > 0) {
    const details = findings.map(({ coordinate, id }) => `${coordinate}: ${id}`).join("\n");
    throw new Error(`${findings.length} known JVM dependency vulnerability finding(s)\n${details}`);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(fs.realpathSync(process.argv[1])).href) {
  const entries = collectMavenDependencies(repository);
  await checkMavenDependencies(entries);
  process.stdout.write(
    `OSV + security floor check: ${entries.length} locked/verified Maven coordinates (runtime, test, build, consumer and wrapper), no known findings\n`,
  );
}
