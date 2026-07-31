import { HocuspocusProvider } from "@hocuspocus/provider";
import * as Y from "yjs";

const port = Number(process.env.HOCUSPOCUS_JVM_PORT ?? "19876");
const url = `ws://127.0.0.1:${port}/collab`;
const documentName = `jvm-oracle-${Date.now()}`;

const expectedAnswerDocument = {
	question: {
		id: 42,
		status: "IN_PROGRESS",
		lastAppliedSourceId: null,
		assignUser: ["user-1", "사용자-😀"],
		answer: {
			type: "doc",
			attrs: { answer_node_ids: ["n4", "선택-😀"] },
			content: [{
				type: "paragraph",
				attrs: { index: 0, node_ids: ["n4"] },
				content: [{ type: "text", text: "저장된 답변 😀" }],
			}],
		},
		lastMutationId: "mutation-1",
	},
	paragraph: {
		name: "paragraph",
		attributes: { index: 0, node_ids: ["n4"] },
		text: "저장된 답변 😀",
		selection: {
			name: "selectionOption",
			attributes: { node_id: "선택-😀", score: 1.5, selected: true },
		},
	},
};

const seedAnswerDocument = document => {
	const question = new Y.Map();
	question.set("id", 42);
	question.set("status", "IN_PROGRESS");
	question.set("lastAppliedSourceId", null);
	question.set("assignUser", ["user-1", "사용자-😀"]);
	question.set("answer", expectedAnswerDocument.question.answer);
	question.set("lastMutationId", "mutation-1");
	document.getMap("questions").set("42", question);

	const paragraph = new Y.XmlElement("paragraph");
	paragraph.setAttribute("index", 0);
	paragraph.setAttribute("node_ids", ["n4"]);
	const text = new Y.XmlText();
	text.insert(0, "저장된 답변 😀");
	paragraph.insert(0, [text]);
	const selection = new Y.XmlElement("selectionOption");
	selection.setAttribute("node_id", "선택-😀");
	selection.setAttribute("selected", true);
	selection.setAttribute("score", 1.5);
	paragraph.insert(1, [selection]);
	document.getXmlFragment("42").insert(0, [paragraph]);
};

const answerDocumentState = document => {
	const question = document.getMap("questions").get("42");
	const paragraph = document.getXmlFragment("42").get(0);
	const text = paragraph instanceof Y.XmlElement ? paragraph.get(0) : null;
	const selection = paragraph instanceof Y.XmlElement ? paragraph.get(1) : null;
	return {
		question: question instanceof Y.Map ? question.toJSON() : null,
		paragraph: paragraph instanceof Y.XmlElement ? {
			name: paragraph.nodeName,
			attributes: paragraph.getAttributes(),
			text: text instanceof Y.XmlText ? text.toString() : null,
			selection: selection instanceof Y.XmlElement ? {
				name: selection.nodeName,
				attributes: selection.getAttributes(),
			} : null,
		} : null,
	};
};

const canonicalJson = value => JSON.stringify(value, (_, nested) => {
	if (nested == null || Array.isArray(nested) || typeof nested !== "object") return nested;
	return Object.fromEntries(Object.entries(nested).sort(([left], [right]) => left.localeCompare(right)));
});

const encodedStateVector = document => Buffer.from(Y.encodeStateVector(document)).toString("base64");

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
let expectedStateVector;
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

	seedAnswerDocument(first.document);
	await waitFor(() => !first.provider.hasUnsyncedChanges, "answer document acknowledgement");
	await waitFor(
		() => (
			canonicalJson(answerDocumentState(second.document)) === canonicalJson(expectedAnswerDocument)
			&& encodedStateVector(second.document) === encodedStateVector(first.document)
		),
		"nested map and Tiptap XML answer document",
	);
	expectedStateVector = encodedStateVector(first.document);

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
	await waitFor(
		() => (
			canonicalJson(answerDocumentState(reconnected.document)) === canonicalJson(expectedAnswerDocument)
			&& encodedStateVector(reconnected.document) === expectedStateVector
		),
		"persisted answer document after reconnect",
	);

	console.log(JSON.stringify({
		protocol: "hocuspocus-v4",
		sessionAwareness: true,
		tokenRefresh: true,
		answerDocument: true,
		stateVector: true,
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
