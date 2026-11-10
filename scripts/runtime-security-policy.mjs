// Release advisories can precede OSV indexing. Keep these floors alongside OSV.
// https://netty.io/news/2026/11/02/4-2-20-Final.html
// https://netty.io/news/2026/11/02/4-1-140-Final.html
// https://logback.qos.ch/news.html#1.6.6 (cyclic throwable deserialization)
const nettyFloors = new Map([["4.1", 140], ["4.2", 20]]);

export function runtimeSecurityFinding({ package: dependency, version }) {
	if (dependency.ecosystem !== "Maven") return null;
	if (dependency.name.startsWith("io.netty:netty-")) {
		const match = version.match(/^(4\.[12])\.(\d+)\.Final$/u);
		if (match && Number(match[2]) < nettyFloors.get(match[1])) {
			return `Netty security release requires >= ${match[1]}.${nettyFloors.get(match[1])}.Final`;
		}
	}
	if (["ch.qos.logback:logback-classic", "ch.qos.logback:logback-core"].includes(dependency.name)) {
		const match = version.match(/^(\d+)\.(\d+)\.(\d+)$/u);
		if (match) {
			const [, major, minor, patch] = match.map(Number);
			if (major < 1 || (major === 1 && (minor < 6 || (minor === 6 && patch < 6)))) {
				return "Logback security floor is 1.6.6 (cyclic throwable deserialization)";
			}
		}
	}
	return null;
}
