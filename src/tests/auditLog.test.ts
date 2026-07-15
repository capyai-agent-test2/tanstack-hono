import { describe, expect, it, beforeEach } from "vite-plus/test";
import { clearAuditLogEntries, listAuditLogEntries, recordAuditLogEntry } from "../lib/auditLog.ts";
import { handler } from "../routes/-api.ts";

describe("audit log", () => {
	beforeEach(() => {
		clearAuditLogEntries();
	});

	it("filters entries by actor", () => {
		recordAuditLogEntry({
			actor: "user-1",
			action: "created",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-15T10:00:00.000Z",
		});
		recordAuditLogEntry({
			actor: "user-2",
			action: "deleted",
			entityType: "project",
			entityId: "project-2",
			timestamp: "2026-07-15T11:00:00.000Z",
		});

		expect(listAuditLogEntries({ actor: "user-1" })).toEqual([
			{
				actor: "user-1",
				action: "created",
				entityType: "project",
				entityId: "project-1",
				timestamp: "2026-07-15T10:00:00.000Z",
			},
		]);
	});

	it("returns entries newest first from the HTTP endpoint", async () => {
		recordAuditLogEntry({
			actor: "user-1",
			action: "created",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-15T10:00:00.000Z",
		});
		recordAuditLogEntry({
			actor: "user-1",
			action: "updated",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-15T12:00:00.000Z",
		});

		const response = await handler.request("/audit-log?actor=user-1");
		const body = await response.json();

		expect(response.status).toBe(200);
		expect(body.entries.map((entry: { action: string }) => entry.action)).toEqual([
			"updated",
			"created",
		]);
	});

	it("caps HTTP limit at 100", async () => {
		for (let index = 0; index < 105; index += 1) {
			recordAuditLogEntry({
				actor: "user-1",
				action: "updated",
				entityType: "project",
				entityId: `project-${index}`,
				timestamp: new Date(Date.UTC(2026, 6, 15, 10, index)).toISOString(),
			});
		}

		const response = await handler.request("/audit-log?actor=user-1&limit=500");
		const body = await response.json();

		expect(response.status).toBe(200);
		expect(body.entries).toHaveLength(100);
		expect(body.entries[0].entityId).toBe("project-104");
	});
});
