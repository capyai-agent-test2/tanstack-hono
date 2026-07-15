import { Hono } from "hono";
import { beforeEach, describe, expect, it } from "vite-plus/test";
import { clearAuditLogEntries, listAuditLogEntries, recordAuditLogEntry } from "../lib/auditLog.ts";
import { handler as apiHandler } from "../routes/-api.ts";

describe("audit log", () => {
	beforeEach(() => {
		clearAuditLogEntries();
	});

	it("filters entries by actor", () => {
		recordAuditLogEntry({
			actor: "tenant-a",
			action: "created",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-15T10:00:00.000Z",
		});
		recordAuditLogEntry({
			actor: "tenant-b",
			action: "updated",
			entityType: "project",
			entityId: "project-2",
			timestamp: "2026-07-15T11:00:00.000Z",
		});

		expect(listAuditLogEntries({ actor: "tenant-a" })).toEqual([
			{
				actor: "tenant-a",
				action: "created",
				entityType: "project",
				entityId: "project-1",
				timestamp: "2026-07-15T10:00:00.000Z",
			},
		]);
	});

	it("returns entries newest first", () => {
		recordAuditLogEntry({
			actor: "tenant-a",
			action: "oldest",
			entityType: "project",
			entityId: "project-1",
			timestamp: "2026-07-15T10:00:00.000Z",
		});
		recordAuditLogEntry({
			actor: "tenant-a",
			action: "newest",
			entityType: "project",
			entityId: "project-2",
			timestamp: "2026-07-15T12:00:00.000Z",
		});
		recordAuditLogEntry({
			actor: "tenant-a",
			action: "middle",
			entityType: "project",
			entityId: "project-3",
			timestamp: "2026-07-15T11:00:00.000Z",
		});

		expect(listAuditLogEntries({ actor: "tenant-a" }).map((entry) => entry.action)).toEqual([
			"newest",
			"middle",
			"oldest",
		]);
	});

	it("caps the HTTP limit parameter at 100", async () => {
		for (let index = 0; index < 120; index += 1) {
			recordAuditLogEntry({
				actor: "tenant-a",
				action: "created",
				entityType: "document",
				entityId: `document-${index}`,
				timestamp: new Date(Date.UTC(2026, 6, 15, 10, index)).toISOString(),
			});
		}

		const app = new Hono().route("/api", apiHandler);
		const response = await app.request("/api/audit-log?actor=tenant-a&limit=200");
		const body = (await response.json()) as unknown[];

		expect(response.status).toBe(200);
		expect(body).toHaveLength(100);
	});
});
