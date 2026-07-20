import { Hono } from "hono";
import { afterEach, describe, expect, it } from "vite-plus/test";
import { clearAuditLog, listAuditLog, recordAuditLog } from "../lib/auditLog.ts";
import { handler as apiHandler } from "../routes/-api.ts";

function createApiApp(): Hono {
	return new Hono().route("/api", apiHandler);
}

describe("audit log", () => {
	afterEach(() => {
		clearAuditLog();
	});

	it("filters entries by actor", () => {
		recordAuditLog({
			actor: "user-1",
			action: "created",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-20T10:00:00.000Z",
		});
		recordAuditLog({
			actor: "user-2",
			action: "deleted",
			entityType: "project",
			entityId: "project-2",
			timestamp: "2026-07-20T11:00:00.000Z",
		});

		expect(listAuditLog({ actor: "user-1" })).toEqual([
			{
				actor: "user-1",
				action: "created",
				entityType: "project",
				entityId: "project-1",
				timestamp: "2026-07-20T10:00:00.000Z",
			},
		]);
	});

	it("returns newest entries first from the HTTP endpoint", async () => {
		recordAuditLog({
			actor: "user-1",
			action: "updated",
			entityType: "task",
			entityId: "task-1",
			timestamp: "2026-07-20T10:00:00.000Z",
		});
		recordAuditLog({
			actor: "user-1",
			action: "completed",
			entityType: "task",
			entityId: "task-2",
			timestamp: "2026-07-20T12:00:00.000Z",
		});

		const response = await createApiApp().request("/api/audit-log?actor=user-1");
		const body = (await response.json()) as { entries: Array<{ entityId: string }> };

		expect(response.status).toBe(200);
		expect(body.entries.map((entry) => entry.entityId)).toEqual(["task-2", "task-1"]);
	});

	it("caps HTTP limit requests at 100 entries", async () => {
		for (let index = 0; index < 105; index += 1) {
			recordAuditLog({
				actor: "user-1",
				action: "viewed",
				entityType: "document",
				entityId: `document-${index}`,
				timestamp: new Date(Date.UTC(2026, 6, 20, 10, index)).toISOString(),
			});
		}

		const response = await createApiApp().request("/api/audit-log?actor=user-1&limit=500");
		const body = (await response.json()) as { entries: Array<{ entityId: string }> };

		expect(response.status).toBe(200);
		expect(body.entries).toHaveLength(100);
	});
});
