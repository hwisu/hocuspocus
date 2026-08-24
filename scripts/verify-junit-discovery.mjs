import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";

const repository = path.resolve(import.meta.dirname, "..");

function walk(directory) {
	return fs.existsSync(directory)
		? fs.readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
			const target = path.join(directory, entry.name);
			return entry.isDirectory() ? walk(target) : [target];
		})
		: [];
}

const kotlinTests = fs
	.readdirSync(repository, { withFileTypes: true })
	.filter((entry) => entry.isDirectory() && entry.name.startsWith("hocuspocus-"))
	.flatMap((entry) => {
		const module = entry.name;
		return walk(path.join(repository, module, "src", "test", "kotlin"))
			.filter((file) => file.endsWith(".kt"))
			.map((file) => ({ module, file }));
	});

let annotatedTests = 0;
let discoveredTests = 0;
let skippedTests = 0;
const mismatches = [];

for (const { module, file } of kotlinTests) {
	const source = fs.readFileSync(file, "utf8");
	const annotations = source.match(/^\s*@Test\s*$/gmu)?.length ?? 0;
	if (annotations === 0) continue;
	const methodNames = [...source.matchAll(/^\s*@Test\s*\r?\n\s*fun\s+(?:`([^`]+)`|(\w+))/gmu)].map(
		(match) => match[1] ?? match[2],
	);
	assert.equal(
		methodNames.length,
		annotations,
		`cannot identify every @Test method in ${file}`,
	);

	const packageName = source.match(/^package\s+([\w.]+)\s*$/mu)?.[1];
	const className = source.match(/^class\s+(\w+)/mu)?.[1];
	assert.ok(packageName && className, `cannot identify test class in ${file}`);
	const qualifiedName = `${packageName}.${className}`;
	const resultFile = path.join(
		repository,
		module,
		"build",
		"test-results",
		"test",
		`TEST-${qualifiedName}.xml`,
	);
	if (!fs.existsSync(resultFile)) {
		mismatches.push(`${qualifiedName}: ${annotations} @Test methods, no JUnit XML result`);
		continue;
	}
	const result = fs.readFileSync(resultFile, "utf8");
	const discovered = Number(result.match(/<testsuite\b[^>]*\btests="(\d+)"/u)?.[1]);
	const skipped = Number(result.match(/<testsuite\b[^>]*\bskipped="(\d+)"/u)?.[1]);
	const failures = Number(result.match(/<testsuite\b[^>]*\bfailures="(\d+)"/u)?.[1]);
	const errors = Number(result.match(/<testsuite\b[^>]*\berrors="(\d+)"/u)?.[1]);
	assert.ok(Number.isInteger(discovered), `cannot read test count from ${resultFile}`);
	assert.ok(Number.isInteger(skipped), `cannot read skipped count from ${resultFile}`);
	assert.equal(failures, 0, `JUnit failures recorded in ${resultFile}`);
	assert.equal(errors, 0, `JUnit errors recorded in ${resultFile}`);
	annotatedTests += annotations;
	discoveredTests += discovered;
	skippedTests += skipped;
	if (annotations !== discovered) {
		mismatches.push(`${qualifiedName}: ${annotations} @Test methods, ${discovered} discovered`);
	}
	const testCaseNames = new Set(
		[...result.matchAll(/<testcase\b[^>]*\bname="([^"]*)"/gu)].map((match) =>
			match[1]
				.replaceAll("&quot;", '"')
				.replaceAll("&apos;", "'")
				.replaceAll("&lt;", "<")
				.replaceAll("&gt;", ">")
				.replaceAll("&amp;", "&")
				.replace(/\(\)$/u, ""),
		),
	);
	for (const methodName of methodNames) {
		if (!testCaseNames.has(methodName)) {
			mismatches.push(`${qualifiedName}: @Test method was not discovered: ${methodName}`);
		}
	}
}

assert.deepEqual(
	mismatches,
	[],
	`Kotlin @Test methods missing from the JUnit run:\n${mismatches.join("\n")}`,
);
process.stdout.write(
	`JUnit discovery: ${discoveredTests}/${annotatedTests} Kotlin @Test methods discovered ` +
		`(${discoveredTests - skippedTests} executed, ${skippedTests} skipped)\n`,
);
