import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { z } from "zod";

const routes = new Hono()
	.get("/livez", (c) => {
		return c.json({
			phaseStatus: "ok",
			checkedAt: new Date().toISOString(),
			uptime: process.uptime(),
			environment: process.env.NODE_ENV || "development",
		});
	})
	.post(
		"/echo-v3",
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
