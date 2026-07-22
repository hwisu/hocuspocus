import { HocuspocusProvider } from "@hocuspocus/provider";
import * as Y from "yjs";

const port = Number(process.env.HOCUSPOCUS_JVM_PORT ?? "19876");
const url = `ws://127.0.0.1:${port}/collab`;
const documentName = `jvm-oracle-${Date.now()}`;

const waitFor = async (predicate, label, timeoutMs = 10_000) => {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await new Promise(resolve => setTimeout(resolve, 20));
	}
	throw new Error(`Timed out waiting for ${label}`);
};

const createClient = () => {
	const document = new Y.Doc();
	const stateless = [];
	let resolveSynced;
	let rejectSynced;
	const synced = new Promise((resolve, reject) => {
		resolveSynced = resolve;
		rejectSynced = reject;
	});
	const provider = new HocuspocusProvider({
		url,
		name: documentName,
		document,
		token: "oracle-token",
		sessionAwareness: true,
		onSynced({ state }) {
			if (state) resolveSynced();
		},
		onAuthenticationFailed({ reason }) {
			rejectSynced(new Error(`Authentication failed: ${reason}`));
		},
		onStateless({ payload }) {
			stateless.push(payload);
		},
	});
	return { document, provider, stateless, synced };
};

const destroyClient = client => {
	if (!client) return;
	client.provider.destroy();
	client.document.destroy();
};

const first = createClient();
const second = createClient();
let reconnected;
let initialClientsDestroyed = false;

try {
	await Promise.all([first.synced, second.synced]);
	await waitFor(
		() => first.stateless.includes("token:oracle-token") && second.stateless.includes("token:oracle-token"),
		"v4 token refresh",
	);

	first.document.getText("body").insert(0, "Ktor ↔ Hocuspocus 😀");
	await waitFor(() => !first.provider.hasUnsyncedChanges, "sync acknowledgement");
	await waitFor(
		() => second.document.getText("body").toString() === "Ktor ↔ Hocuspocus 😀",
		"cross-provider Yjs update",
	);

	first.provider.setAwarenessField("user", { name: "Ada" });
	await waitFor(
		() => [...second.provider.awareness.getStates().values()]
			.some(state => state?.user?.name === "Ada"),
		"awareness broadcast",
	);
	first.provider.awareness.setLocalState(null);
	await waitFor(
		() => ![...second.provider.awareness.getStates().values()]
			.some(state => state?.user?.name === "Ada"),
		"awareness removal",
	);

	first.provider.sendStateless("oracle-stateless");
	await waitFor(
		() => second.stateless.includes("oracle-stateless"),
		"stateless broadcast",
	);

	destroyClient(first);
	destroyClient(second);
	initialClientsDestroyed = true;
	await waitFor(
		async () => (await fetch(`http://127.0.0.1:${port}/documents-count`).then(response => response.text())) === "0",
		"document store and unload",
	);

	reconnected = createClient();
	await reconnected.synced;
	await waitFor(
		() => reconnected.document.getText("body").toString() === "Ktor ↔ Hocuspocus 😀",
		"persisted Yjs state after reconnect",
	);

	console.log(JSON.stringify({
		protocol: "hocuspocus-v4",
		sessionAwareness: true,
		tokenRefresh: true,
		yjs: second.document.getText("body").toString(),
		awareness: true,
		awarenessRemoval: true,
		stateless: true,
		persistence: true,
		reconnect: true,
	}));
} finally {
	if (!initialClientsDestroyed) {
		destroyClient(first);
		destroyClient(second);
	}
	destroyClient(reconnected);
}
