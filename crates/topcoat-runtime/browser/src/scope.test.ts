import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { Runtime } from "./runtime";
import { ReactiveScope } from "./scope";
import { String as RuntimeString } from "./surrogate/string";

class MockWebSocket {
	static readonly CONNECTING = 0;
	static readonly OPEN = 1;
	static readonly CLOSING = 2;
	static readonly CLOSED = 3;
	static instances: MockWebSocket[] = [];

	readonly CONNECTING = MockWebSocket.CONNECTING;
	readonly OPEN = MockWebSocket.OPEN;
	readonly CLOSING = MockWebSocket.CLOSING;
	readonly CLOSED = MockWebSocket.CLOSED;
	readonly sent: string[] = [];
	readonly url: string;
	readyState = MockWebSocket.CONNECTING;
	onopen: ((event: Event) => unknown) | null = null;
	onmessage: ((event: MessageEvent) => unknown) | null = null;
	onclose: ((event: CloseEvent) => unknown) | null = null;
	onerror: ((event: Event) => unknown) | null = null;
	closeCalls = 0;

	constructor(url: string | URL) {
		this.url = url.toString();
		MockWebSocket.instances.push(this);
	}

	open(): void {
		this.readyState = MockWebSocket.OPEN;
		this.onopen?.(new Event("open"));
	}

	send(value: string): void {
		this.sent.push(value);
	}

	message(value: string): void {
		this.onmessage?.(new MessageEvent("message", { data: value }));
	}

	close(code = 1000): void {
		this.closeCalls += 1;
		this.readyState = MockWebSocket.CLOSED;
		this.onclose?.(new CloseEvent("close", { code, wasClean: code === 1000 }));
	}

	unexpectedClose(): void {
		this.readyState = MockWebSocket.CLOSED;
		this.onclose?.(new CloseEvent("close", { code: 1006, wasClean: false }));
	}
}

const insertedHtml: string[] = [];
const parent = {
	insertBefore(fragment: { html: string }) {
		insertedHtml.push(fragment.html);
	},
	removeChild() {},
};
const end = { parentNode: parent } as unknown as Comment;
const start = {
	parentNode: parent,
	nextSibling: end,
} as unknown as Comment;

Object.defineProperty(globalThis, "window", {
	configurable: true,
	value: { location: new URL("http://example.test/page") },
});
Object.defineProperty(globalThis, "document", {
	configurable: true,
	value: {
		createRange: () => ({
			createContextualFragment: (html: string) => ({ html }),
		}),
		createTreeWalker: () => ({
			currentNode: parent,
			nextNode: () => null,
		}),
	},
});
Object.defineProperty(globalThis, "NodeFilter", {
	configurable: true,
	value: { SHOW_COMMENT: 128, SHOW_ELEMENT: 1 },
});
Object.defineProperty(globalThis, "Node", {
	configurable: true,
	value: { ELEMENT_NODE: 1 },
});
Object.defineProperty(globalThis, "WebSocket", {
	configurable: true,
	value: MockWebSocket,
});
Object.defineProperty(globalThis, "Event", {
	configurable: true,
	value: class Event {
		constructor(readonly type: string) {}
	},
});
Object.defineProperty(globalThis, "MessageEvent", {
	configurable: true,
	value: class MessageEvent {
		constructor(
			readonly type: string,
			readonly init: { data: unknown },
		) {}

		get data(): unknown {
			return this.init.data;
		}
	},
});
Object.defineProperty(globalThis, "CloseEvent", {
	configurable: true,
	value: class CloseEvent {
		readonly code: number;
		readonly wasClean: boolean;

		constructor(
			readonly type: string,
			init: { code?: number; wasClean?: boolean } = {},
		) {
			this.code = init.code ?? 0;
			this.wasClean = init.wasClean ?? false;
		}
	},
});

beforeEach(() => {
	MockWebSocket.instances = [];
	insertedHtml.length = 0;
});

afterEach(() => {
	vi.useRealTimers();
});

function createWebSocketScope(): { runtime: Runtime; scope: ReactiveScope } {
	const runtime = new Runtime();
	runtime.registry.insert("query", new RuntimeString("first"));
	const scope = new ReactiveScope(
		runtime.rootScope,
		runtime,
		"scope-id",
		"/_topcoat/shards/id",
		"ws",
		['cx.signal("query").get()'],
		start,
	);
	scope.attachEnd(end);
	scope.startWatching();
	return { runtime, scope };
}

it("streams arguments and replaces content until disposal", async () => {
	const { runtime, scope } = createWebSocketScope();
	const socket = MockWebSocket.instances[0];
	expect(socket).toBeDefined();
	expect(socket?.url).toBe("ws://example.test/_topcoat/shards/id");
	expect(socket?.sent).toEqual([]);

	socket?.open();
	expect(socket?.sent).toEqual(['["first"]']);

	const query = runtime.registry.handle("query");
	query.set(new RuntimeString("second"));
	query.set(new RuntimeString("third"));
	await Promise.resolve();
	await Promise.resolve();
	expect(socket?.sent).toEqual(['["first"]', '["third"]']);

	socket?.message("<p>first push</p>");
	socket?.message("<p>second push</p>");
	expect(insertedHtml).toEqual(["<p>first push</p>", "<p>second push</p>"]);

	scope.dispose();
	expect(socket?.closeCalls).toBe(1);
	query.set(new RuntimeString("after disposal"));
	await Promise.resolve();
	await Promise.resolve();
	expect(socket?.sent).toEqual(['["first"]', '["third"]']);
});

it("reconnects unexpected closes with bounded backoff and current state", () => {
	vi.useFakeTimers();
	const { runtime, scope } = createWebSocketScope();
	let socket = MockWebSocket.instances[0];
	socket?.open();
	socket?.message("<p>hydrated</p>");
	expect(socket?.sent).toEqual(['["first"]']);

	socket?.unexpectedClose();
	vi.advanceTimersByTime(249);
	expect(MockWebSocket.instances).toHaveLength(1);

	const query = runtime.registry.handle("query");
	query.set(new RuntimeString("while disconnected"));
	vi.advanceTimersByTime(1);
	expect(MockWebSocket.instances).toHaveLength(2);
	socket = MockWebSocket.instances[1];
	socket?.open();
	expect(socket?.sent).toEqual(['["while disconnected"]']);

	for (const delay of [500, 1000, 2000, 4000, 8000, 10_000, 10_000]) {
		socket?.unexpectedClose();
		const count = MockWebSocket.instances.length;
		vi.advanceTimersByTime(delay - 1);
		expect(MockWebSocket.instances).toHaveLength(count);
		vi.advanceTimersByTime(1);
		expect(MockWebSocket.instances).toHaveLength(count + 1);
		socket = MockWebSocket.instances.at(-1);
	}

	scope.dispose();
});

it("does not reconnect normal closes or disposed scopes", () => {
	vi.useFakeTimers();
	const normal = createWebSocketScope();
	MockWebSocket.instances[0]?.close(1000);
	vi.advanceTimersByTime(20_000);
	expect(MockWebSocket.instances).toHaveLength(1);
	normal.scope.dispose();

	MockWebSocket.instances = [];
	const unexpected = createWebSocketScope();
	MockWebSocket.instances[0]?.unexpectedClose();
	unexpected.scope.dispose();
	vi.advanceTimersByTime(20_000);
	expect(MockWebSocket.instances).toHaveLength(1);
});
