import { Hono } from "hono";
import { describe, expect, it } from "vite-plus/test";
import { handler as apiHandler } from "../routes/-api";

const handler = new Hono().route("/api", apiHandler);

type EchoErrorBody = {
	error: {
		code: string;
		message: string;
		issues: Array<{
			path: string[];
			code: string;
			message: string;
		}>;
	};
};

const postEcho = (body: BodyInit, contentType = "application/json") =>
	handler.request("/api/echo", {
		method: "POST",
		headers: {
			"Content-Type": contentType,
		},
		body,
	});

const expectEchoError = async (response: Response) => {
	expect(response.status).toBe(400);
	expect(response.headers.get("content-type")).toContain("application/json");

	const body = (await response.json()) as EchoErrorBody;
	expect(Object.keys(body)).toEqual(["error"]);
	expect(Object.keys(body.error)).toEqual(["code", "message", "issues"]);
	expect(body.error.code).toBe("invalid_request_body");
	expect(body.error.message).toBe("Invalid request body");
	expect(Array.isArray(body.error.issues)).toBe(true);
	expect(body.error.issues.length).toBeGreaterThan(0);

	return body;
};

describe("POST /api/echo", () => {
	it("returns the echo response for a valid body", async () => {
		const response = await postEcho(JSON.stringify({ message: "hello" }));

		expect(response.status).toBe(200);
		expect(response.headers.get("content-type")).toContain("application/json");

		const body = (await response.json()) as { echo: string; receivedAt: string };
		expect(body.echo).toBe("hello");
		expect(new Date(body.receivedAt).toISOString()).toBe(body.receivedAt);
	});

	it("returns JSON 400 for malformed JSON", async () => {
		const body = await expectEchoError(await postEcho('{"message":'));

		expect(body.error.issues).toEqual([
			{
				path: [],
				code: "invalid_json",
				message: "Malformed JSON in request body",
			},
		]);
	});

	it.each(["Application/JSON", "application/vnd.api.v2+json"])(
		"returns JSON 400 for malformed JSON with %s content type",
		async (contentType) => {
			const body = await expectEchoError(await postEcho('{"message":', contentType));

			expect(body.error.issues[0]?.code).toBe("invalid_json");
		}
	);

	it.each([
		["missing message", {}],
		["empty message", { message: "" }],
		["wrong-type message", { message: 123 }],
	])("returns JSON 400 for %s", async (_name, payload) => {
		const body = await expectEchoError(await postEcho(JSON.stringify(payload)));

		expect(body.error.issues[0]?.path).toEqual(["message"]);
		expect(body.error.issues[0]?.code).toEqual(expect.any(String));
		expect(body.error.issues[0]?.message).toEqual(expect.any(String));
	});
});
