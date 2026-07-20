import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { z } from "zod";
import { listAuditLog } from "../lib/auditLog.ts";

function parseLimit(value: string | undefined): number {
	if (!value) return 100;

	const limit = Number.parseInt(value, 10);
	if (Number.isNaN(limit)) return 100;

	return Math.min(limit, 100);
}

const routes = new Hono()
	.get("/health", (c) => {
		return c.json({
			status: "ok",
			timestamp: new Date().toISOString(),
			uptime: process.uptime(),
			environment: process.env.NODE_ENV || "development",
		});
	})
	.get("/audit-log", (c) => {
		const actor = c.req.query("actor");
		const limit = parseLimit(c.req.query("limit"));

		return c.json({
			entries: listAuditLog({ actor, limit }),
		});
	})
	.post(
		"/echo",
		zValidator(
			"json",
			z.object({
				message: z.string().min(1),
			})
		),
		(c) => {
			const { message } = c.req.valid("json");
			return c.json({
				echo: message,
				receivedAt: new Date().toISOString(),
			});
		}
	);

export type ApiRoutes = typeof routes;
export const handler = routes;
