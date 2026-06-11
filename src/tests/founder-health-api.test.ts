import { Hono } from "hono";
import { describe, expect, it } from "vite-plus/test";
import { handler as apiHandler } from "../routes/-api";
import type { FounderHealthResponse } from "../routes/-api";

const app = new Hono().route("/api", apiHandler);

describe("Founder Health API", () => {
	it("returns a deterministic executive-ready founder health snapshot", async () => {
		const firstResponse = await app.request("/api/founder-health");
		const secondResponse = await app.request("/api/founder-health");

		expect(firstResponse.status).toBe(200);
		expect(firstResponse.headers.get("content-type")).toContain("application/json");

		const firstBody = (await firstResponse.json()) as FounderHealthResponse;
		const secondBody = (await secondResponse.json()) as FounderHealthResponse;

		expect(firstBody).toEqual(secondBody);
		expect(firstBody.asOf).toBe("2026-05-31T00:00:00.000Z");
		expect(firstBody.overallScore).toBe(82);
		expect(firstBody.executiveSummary).toContain("board narrative");
		expect(firstBody.domains.map(({ domain }) => domain)).toEqual([
			"growth",
			"activation",
			"retention",
			"reliability",
		]);
		expect(firstBody.domains).toHaveLength(4);
		expect(firstBody.domains[1].recommendedAction).toContain("executive onboarding checkpoint");
	});
});
