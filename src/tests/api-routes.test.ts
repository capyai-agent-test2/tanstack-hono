import { describe, expect, it } from "vite-plus/test";
import { handler } from "../routes/-api";

describe("API routes", () => {
	it("does not expose environment variables or authorization headers", async () => {
		const response = await handler.request("/debug/env", {
			headers: {
				authorization: "Bearer secret-token",
			},
		});

		expect(response.status).toBe(404);
	});
});
