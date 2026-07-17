import { Server } from "../../packages/server/dist/hocuspocus-server.esm.js";

const port = Number(process.env.HOCUSPOCUS_BENCHMARK_PORT ?? "19876");
const server = new Server({
	address: "127.0.0.1",
	port,
	quiet: true,
	stopOnSignals: false,
});

await server.listen();
process.stdout.write(`${JSON.stringify({ ready: true, port })}\n`);

let stopping = false;
const stop = async () => {
	if (stopping) return;
	stopping = true;
	server.hocuspocus.closeConnections();
	await server.destroy();
};

for (const signal of ["SIGINT", "SIGTERM"]) {
	process.on(signal, () => {
		stop()
			.then(() => process.exit(0))
			.catch((error) => {
				console.error(error);
				process.exit(1);
			});
	});
}
