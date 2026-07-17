import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";

const repository = path.resolve(import.meta.dirname, "../..");
const matrixPath = path.join(repository, "jvm/upstream-server-test-matrix.json");
const matrix = JSON.parse(fs.readFileSync(matrixPath, "utf8"));
const sourceDirectory = path.join(repository, matrix.sourceDirectory);
const sourceFiles = fs.readdirSync(sourceDirectory)
  .filter((name) => name.endsWith(".ts"))
  .sort();
const entries = matrix.entries;
const mappedFiles = entries.map(({ source }) => source).sort();
const strategyNames = new Set(Object.keys(matrix.strategies));
const scenarioPattern = /^\s*(?:test|it)\(/gmu;
const disabledScenarioPattern = /^\s*(?:test|it)\.(?:only|skip|todo)\(/gmu;

assert.equal(new Set(mappedFiles).size, mappedFiles.length, "matrix contains duplicate source files");
assert.deepEqual(
  mappedFiles,
  sourceFiles,
  "every upstream server test file must have exactly one JVM/Ktor owner",
);

let activeScenarios = 0;
const targetTests = new Map();
for (const entry of entries) {
  assert.ok(
    entry.owner === "core" || entry.owner === "ktor",
    `invalid owner for ${entry.source}`,
  );
  assert.ok(strategyNames.has(entry.strategy), `invalid strategy for ${entry.source}`);
  assert.equal(
    entry.strategy,
    entry.owner === "ktor" ? "ktor-native" : "behavioral-contract",
    `owner and strategy disagree for ${entry.source}`,
  );
  const target = path.join(repository, entry.target);
  assert.ok(fs.existsSync(target), `missing JVM target for ${entry.source}: ${entry.target}`);
  const targetSource = fs.readFileSync(target, "utf8");
  const tests = targetSource.match(/^\s*@Test\s*$/gmu)?.length ?? 0;
  assert.ok(tests > 0, `JVM target contains no active @Test methods: ${entry.target}`);
  targetTests.set(entry.target, tests);
  const source = fs.readFileSync(path.join(sourceDirectory, entry.source), "utf8");
  const disabledScenarios = source.match(disabledScenarioPattern)?.length ?? 0;
  assert.equal(disabledScenarios, 0, `disabled or focused upstream scenarios require explicit handling: ${entry.source}`);
  const scenarios = source.match(scenarioPattern)?.length ?? 0;
  assert.equal(scenarios, entry.scenarios, `upstream scenario count drifted for ${entry.source}`);
  activeScenarios += scenarios;
}

assert.ok(activeScenarios > 0, "no upstream server scenarios were found");
assert.equal(activeScenarios, matrix.expectedScenarioCount, "total upstream scenario count drifted");
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
const activeJvmTests = [...targetTests.values()].reduce((sum, count) => sum + count, 0);
process.stdout.write(
  `upstream server matrix: ${entries.length} files, ${activeScenarios} explicitly classified upstream scenarios, ` +
    `${activeJvmTests} active JVM contract tests across ${targetTests.size} targets (grouped, not one-to-one), ` +
    `${entries.filter(({ owner }) => owner === "core").length} core-owned, ` +
    `${entries.filter(({ owner }) => owner === "ktor").length} Ktor-owned\n`,
);
