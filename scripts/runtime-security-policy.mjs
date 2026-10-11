// Release advisories can precede OSV indexing. Keep these floors alongside OSV.
// https://github.com/netty/netty/releases/tag/netty-4.2.19.Final
// https://github.com/netty/netty/releases/tag/netty-4.1.139.Final
// https://logback.qos.ch/news.html#1.6.5 (CVE-2026-104721)
const nettyFloors = new Map([["4.1", 139], ["4.2", 19]]);

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
			if (major < 1 || (major === 1 && (minor < 6 || (minor === 6 && patch < 5)))) {
				return "CVE-2026-104721: Logback security floor is 1.6.5";
			}
		}
	}
	return null;
}
