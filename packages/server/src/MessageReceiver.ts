import { AuthMessageType } from "@hocuspocus/common";
import * as decoding from "lib0/decoding";
import { readVarString } from "lib0/decoding";
import * as encoding from "lib0/encoding";
import { applyAwarenessUpdate } from "y-protocols/awareness";
import {
	messageYjsSyncStep1,
	messageYjsSyncStep2,
	messageYjsUpdate,
	readSyncStep1,
	readSyncStep2,
	readUpdate,
} from "y-protocols/sync";
import * as Y from "yjs";
import type Connection from "./Connection.ts";
import type Document from "./Document.ts";
import type { IncomingMessage } from "./IncomingMessage.ts";
import { OutgoingMessage } from "./OutgoingMessage.ts";
import {
	MessageType,
	type ConnectionTransactionOrigin,
	type TransactionOrigin,
} from "./types.ts";

type DecodedAwarenessEntry = {
	clientId: number;
	clock: number;
	state: Record<string, any> | null;
};

const decodeAwarenessEntries = (
	update: Uint8Array,
): Map<number, DecodedAwarenessEntry> => {
	const decoder = decoding.createDecoder(update);
	const entries = new Map<number, DecodedAwarenessEntry>();
	const count = decoding.readVarUint(decoder);

	for (let index = 0; index < count; index += 1) {
		const clientId = decoding.readVarUint(decoder);
		const clock = decoding.readVarUint(decoder);
		const state = JSON.parse(decoding.readVarString(decoder)) as
			| Record<string, any>
			| null;
		entries.set(clientId, { clientId, clock, state });
	}

	return entries;
};

const encodeAwarenessEntries = (
	entries: Iterable<DecodedAwarenessEntry>,
): Uint8Array => {
	const values = Array.from(entries);
	const encoder = encoding.createEncoder();
	encoding.writeVarUint(encoder, values.length);

	for (const { clientId, clock, state } of values) {
		const serializedState = JSON.stringify(state);
		if (serializedState === undefined) {
			throw new TypeError(
				`Awareness state for client ${clientId} is not JSON serializable`,
			);
		}
		encoding.writeVarUint(encoder, clientId);
		encoding.writeVarUint(encoder, clock);
		encoding.writeVarString(encoder, serializedState);
	}

	return encoding.toUint8Array(encoder);
};

export class MessageReceiver {
	message: IncomingMessage;

	defaultTransactionOrigin?: TransactionOrigin;

	constructor(
		message: IncomingMessage,
		defaultTransactionOrigin?: TransactionOrigin,
	) {
		this.message = message;
		this.defaultTransactionOrigin = defaultTransactionOrigin;
	}

	public async apply(
		document: Document,
		connection?: Connection,
		reply?: (message: Uint8Array) => void,
	) {
		const { message } = this;
		const type = message.readVarUint();
		const emptyMessageLength = message.length;

		switch (type) {
			case MessageType.Sync:
			case MessageType.SyncReply: {
				message.writeVarUint(MessageType.Sync);
				await this.readSyncMessage(
					message,
					document,
					connection,
					reply,
					type !== MessageType.SyncReply,
				);

				if (message.length > emptyMessageLength + 1) {
					if (reply) {
						reply(message.toUint8Array());
					} else if (connection) {
						connection.send(message.toUint8Array());
					}
				}

				break;
			}
			case MessageType.Awareness: {
				let update = message.readVarUint8Array();

				const origin: TransactionOrigin = connection
					? ({
							source: "connection",
							connection,
						} satisfies ConnectionTransactionOrigin)
					: (this.defaultTransactionOrigin ?? { source: "local" });

				// Decode the wire entries directly. Applying them to a temporary
				// Awareness loses null tombstones because getStates() exposes only
				// live states, and also introduces the temporary document's own
				// local awareness state.
				const originalEntries = decodeAwarenessEntries(update);
				const states = new Map<number, Record<string, any>>();
				for (const { clientId, state } of originalEntries.values()) {
					if (state !== null) {
						states.set(clientId, state);
					}
				}

				await document.callbacks.beforeHandleAwareness(
					document,
					states,
					origin,
				);

				const rewrittenEntries: DecodedAwarenessEntry[] = [];
				for (const original of originalEntries.values()) {
					const rewrittenState = states.get(original.clientId);
					if (rewrittenState !== undefined) {
						rewrittenEntries.push({
							...original,
							state: rewrittenState,
						});
					} else if (original.state === null) {
						// Tombstones are intentionally not exposed as mutable
						// states, but they must survive the hook round-trip.
						rewrittenEntries.push(original);
					}
				}
				for (const [clientId, state] of states) {
					if (originalEntries.has(clientId)) {
						continue;
					}
					rewrittenEntries.push({
						clientId,
						clock:
							(document.awareness.meta.get(clientId)?.clock ?? 0) + 1,
						state,
					});
				}
				update = encodeAwarenessEntries(rewrittenEntries);
				applyAwarenessUpdate(document.awareness, update, origin);

				break;
			}
			case MessageType.QueryAwareness: {
				this.applyQueryAwarenessMessage(document, connection, reply);

				break;
			}
			case MessageType.Stateless: {
				connection?.callbacks.statelessCallback({
					connection,
					documentName: document.name,
					document,
					payload: readVarString(message.decoder),
				});

				break;
			}
			case MessageType.BroadcastStateless: {
				// Server-internal opcode used by @hocuspocus/extension-redis to
				// fan a stateless payload across server instances. The Redis path
				// invokes MessageReceiver without a `connection`, so a defined
				// `connection` here means this frame came from a WebSocket client
				// — which is never legitimate. Clients must use MessageType.Stateless
				// (opcode 5); the onStateless hook is the authorization point and
				// may call Document.broadcastStateless() to fan out if appropriate.
				if (connection) {
					throw new Error(
						"BroadcastStateless is a server-internal opcode and cannot be sent from a client",
					);
				}
				const msg = message.readVarString();
				document.getConnections().forEach((c) => {
					c.sendStateless(msg);
				});
				break;
			}

			case MessageType.CLOSE: {
				connection?.close({
					code: 1000,
					reason: "provider_initiated",
				});
				break;
			}

			case MessageType.Auth: {
				const authType = message.readVarUint();
				if (authType === AuthMessageType.Token) {
					connection?.callbacks.onTokenSyncCallback({
						token: message.readVarString(),
					});
					break;
				}
				console.error(
					"Received an authentication message on a connection that is already fully authenticated. Probably your provider has been destroyed + recreated really fast.",
				);
				break;
			}

			default:
				console.error(
					`Unable to handle message of type ${type}: no handler defined! Are your provider/server versions aligned?`,
				);
			// Do nothing
		}
	}

	async readSyncMessage(
		message: IncomingMessage,
		document: Document,
		connection?: Connection,
		reply?: (message: Uint8Array) => void,
		requestFirstSync = true,
	) {
		const type = message.readVarUint();
		const messageAddress = connection?.messageAddress ?? document.name;

		if (connection) {
			await connection.callbacks.beforeSync(connection, {
				type,
				payload: message.peekVarUint8Array(),
			});
		}

		switch (type) {
			case messageYjsSyncStep1: {
				readSyncStep1(message.decoder, message.encoder, document);

				// When the server receives SyncStep1, it should reply with SyncStep2 immediately followed by SyncStep1.
				if (reply && requestFirstSync) {
					const syncMessage = new OutgoingMessage(messageAddress)
						.createSyncReplyMessage()
						.writeFirstSyncStepFor(document);

					reply(syncMessage.toUint8Array());
				} else if (connection) {
					const syncMessage = new OutgoingMessage(messageAddress)
						.createSyncMessage()
						.writeFirstSyncStepFor(document);

					connection.send(syncMessage.toUint8Array());
				}
				break;
			}
			case messageYjsSyncStep2: {
				if (connection?.readOnly) {
					// We're in read-only mode, so we can't apply the update.
					// Let's use snapshotContainsUpdate to see if the update actually contains changes.
					// If not, we can still ack the update
					const snapshot = Y.snapshot(document);
					const update = decoding.readVarUint8Array(message.decoder);
					if (Y.snapshotContainsUpdate(snapshot, update)) {
						// no new changes in update
						const ackMessage = new OutgoingMessage(
							messageAddress,
						).writeSyncStatus(true);

						connection.send(ackMessage.toUint8Array());
					} else {
						// new changes in update that we can't apply, because readOnly
						const ackMessage = new OutgoingMessage(
							messageAddress,
						).writeSyncStatus(false);

						connection.send(ackMessage.toUint8Array());
					}
					break;
				}

				readSyncStep2(
					message.decoder,
					document,
					connection
						? { source: "connection" as const, connection }
						: (this.defaultTransactionOrigin ?? { source: "local" as const }),
				);

				if (connection) {
					connection.send(
						new OutgoingMessage(messageAddress)
							.writeSyncStatus(true)
							.toUint8Array(),
					);
				}
				break;
			}
			case messageYjsUpdate: {
				if (connection?.readOnly) {
					connection.send(
						new OutgoingMessage(messageAddress)
							.writeSyncStatus(false)
							.toUint8Array(),
					);
					break;
				}

				readUpdate(
					message.decoder,
					document,
					connection
						? { source: "connection" as const, connection }
						: (this.defaultTransactionOrigin ?? { source: "local" as const }),
				);
				if (connection) {
					connection.send(
						new OutgoingMessage(messageAddress)
							.writeSyncStatus(true)
							.toUint8Array(),
					);
				}
				break;
			}
			default:
				throw new Error(`Received a message with an unknown type: ${type}`);
		}

		return type;
	}

	applyQueryAwarenessMessage(
		document: Document,
		connection?: Connection,
		reply?: (message: Uint8Array) => void,
	) {
		const message = new OutgoingMessage(
			connection?.messageAddress ?? document.name,
		).createAwarenessUpdateMessage(document.awareness);

		if (reply) {
			reply(message.toUint8Array());
		}
	}
}
