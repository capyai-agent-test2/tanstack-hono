import { Hono } from "hono";
import { describe, expect, it } from "vite-plus/test";
import { handler as apiHandler } from "../routes/-api.ts";
import { getReleaseReadiness, handler } from "../routes/-release-readiness.ts";

describe("release readiness", () => {
	it("returns deterministic readiness data from the standalone handler", async () => {
		const response = await handler.request("/release-readiness");
		const firstBody = await response.json();
		const secondBody = getReleaseReadiness();

		expect(response.status).toBe(200);
		expect(firstBody).toEqual(secondBody);
		expect(firstBody).toEqual({
			status: "ready",
			summary: {
				total: 4,
				passed: 4,
				failed: 0,
			},
			checks: [
				{
					id: "environment.defaults",
					status: "pass",
					message: "Server environment variables have deterministic application defaults.",
				},
				{
					id: "build.assets.expected",
					status: "pass",
					message: "Production build expects client manifest and server entry assets.",
				},
				{
					id: "api.route.mount",
					status: "pass",
					message: "Release readiness handler is mounted under the API route prefix.",
				},
				{
					id: "runtime.metadata",
					status: "pass",
					message: "Runtime and dependency metadata are declared with stable values.",
				},
			],
			metadata: {
				runtime: "node",
				serverFramework: "hono",
				toolchain: "vite-plus",
				routeMount: "/api/release-readiness",
			},
		});
	});

	it("is mounted under the API route prefix", async () => {
		const api = new Hono().route("/api", handler);
		const response = await api.request("/api/release-readiness");
		const body = await response.json();

		expect(response.status).toBe(200);
		expect(body.status).toBe("ready");
		expect(body.summary).toEqual({
			total: 4,
			passed: 4,
			failed: 0,
		});
	});

	it("is wired into the existing API handler", async () => {
		const api = new Hono().route("/api", apiHandler);
		const response = await api.request("/api/release-readiness");
		const body = await response.json();

		expect(response.status).toBe(200);
		expect(body.metadata.routeMount).toBe("/api/release-readiness");
		expect(body.checks.map((check: { id: string }) => check.id)).toEqual([
			"environment.defaults",
			"build.assets.expected",
			"api.route.mount",
			"runtime.metadata",
		]);
	});
});
