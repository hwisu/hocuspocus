import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const repository = path.resolve(import.meta.dirname, "..");
const matrixPath = path.join(repository, "upstream-server-test-matrix.json");
const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
const packageManifest = JSON.parse(
	fs.readFileSync(path.join(repository, "package.json"), "utf8"),
);
const installedUpstreamManifest = JSON.parse(
	fs.readFileSync(
		path.join(repository, "node_modules", matrix.upstream.package, "package.json"),
		"utf8",
	),
);
const installedExtensionManifests = matrix.upstream.extensionPackages.map((packageName) => [
	packageName,
	JSON.parse(
		fs.readFileSync(
			path.join(repository, "node_modules", packageName, "package.json"),
			"utf8",
		),
	),
]);
const serverEntries = matrix.entries;
const extensionEntries = matrix.extensionEntries;
const entries = [...serverEntries, ...extensionEntries];
const mappedFiles = entries.map(({ source }) => source).sort();
const strategyNames = new Set(Object.keys(matrix.strategies));

assert.equal(
	packageManifest.devDependencies[matrix.upstream.package],
	matrix.upstream.version,
	"the compatibility matrix must match the pinned upstream server package",
);
assert.equal(installedUpstreamManifest.version, matrix.upstream.version, "installed upstream version drifted");
assert.equal(installedUpstreamManifest.gitHead, matrix.upstream.gitHead, "installed upstream source commit drifted");
for (const [packageName, manifest] of installedExtensionManifests) {
	assert.equal(
		packageManifest.devDependencies[packageName],
		matrix.upstream.version,
		`${packageName} must be pinned to the matrix version`,
	);
	assert.equal(manifest.version, matrix.upstream.version, `${packageName} installed version drifted`);
	assert.equal(manifest.gitHead, matrix.upstream.gitHead, `${packageName} source commit drifted`);
}
assert.deepEqual(
	[...new Set(extensionEntries.map(({ source }) => source.split("/")[0]))].sort(),
	matrix.upstream.extensionPackages.map((packageName) => packageName.split("/").at(-1)).sort(),
	"extension matrix sources must match the pinned extension packages",
);
assert.equal(
	new Set(mappedFiles).size,
	mappedFiles.length,
	"matrix contains duplicate source files",
);

let activeServerScenarios = 0;
let activeExtensionScenarios = 0;
const targetTests = new Map();
for (const entry of entries) {
	assert.ok(
		["core", "ktor", "redis", "s3", "throttle"].includes(entry.owner),
		`invalid owner for ${entry.source}`,
	);
	assert.ok(
		strategyNames.has(entry.strategy),
		`invalid strategy for ${entry.source}`,
	);
	const expectedStrategy =
		entry.owner === "ktor"
			? "ktor-native"
			: entry.owner === "core"
				? "behavioral-contract"
				: "extension-contract";
	assert.equal(entry.strategy, expectedStrategy, `owner and strategy disagree for ${entry.source}`);
	assert.ok(
		Number.isInteger(entry.scenarios) && entry.scenarios > 0,
		`invalid pinned scenario count for ${entry.source}`,
	);
	if (entry.strategy === "extension-contract") {
		activeExtensionScenarios += entry.scenarios;
	} else {
		activeServerScenarios += entry.scenarios;
	}

	const target = path.join(repository, entry.target);
	assert.ok(
		fs.existsSync(target),
		`missing JVM target for ${entry.source}: ${entry.target}`,
	);
	const targetSource = fs.readFileSync(target, "utf8");
	const tests = targetSource.match(/^\s*@Test\s*$/gmu)?.length ?? 0;
	assert.ok(
		tests > 0,
		`JVM target contains no active @Test methods: ${entry.target}`,
	);
	assert.ok(
		targetSource.includes(`fun \`${entry.contract}\``),
		`named JVM contract is missing for ${entry.source}: ${entry.contract}`,
	);
	targetTests.set(entry.target, tests);
}

assert.ok(activeServerScenarios > 0, "no pinned upstream server scenarios were found");
assert.equal(
	activeServerScenarios,
	matrix.expectedScenarioCount,
	"total pinned upstream scenario count drifted",
);
assert.equal(
	activeExtensionScenarios,
	matrix.expectedExtensionScenarioCount,
	"total pinned upstream extension scenario count drifted",
);
assert.deepEqual(
	[...targetTests.keys()].sort(),
	Object.keys(matrix.targetMinimumTests).sort(),
	"every target must have an explicit JVM contract-test floor",
);
for (const [target, minimum] of Object.entries(matrix.targetMinimumTests)) {
	assert.ok(
		targetTests.get(target) >= minimum,
		`JVM contract-test floor regressed for ${target}: expected at least ${minimum}, got ${targetTests.get(target)}`,
	);
}

const activeJvmTests = [...targetTests.values()].reduce(
	(sum, count) => sum + count,
	0,
);
process.stdout.write(
	`upstream ${matrix.upstream.package} ${matrix.upstream.version} matrix: ` +
			`${serverEntries.length} server files/${activeServerScenarios} scenarios and ` +
			`${extensionEntries.length} extension files/${activeExtensionScenarios} scenarios, ` +
			`${activeJvmTests} active JVM contract tests across ${targetTests.size} targets (grouped, not one-to-one), ` +
			`${serverEntries.filter(({ owner }) => owner === "core").length} core-owned, ` +
			`${serverEntries.filter(({ owner }) => owner === "ktor").length} Ktor-owned\n`,
);
