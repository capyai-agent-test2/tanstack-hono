import { zValidator } from "@hono/zod-validator";
import type { Context } from "hono";
import { Hono } from "hono";
import { createMiddleware } from "hono/factory";
import { z } from "zod";

type EchoValidationIssue = {
	path: string[];
	code: string;
	message: string;
};

const echoValidationError = (c: Context, issues: EchoValidationIssue[]) =>
	c.json(
		{
			error: {
				code: "invalid_request_body",
				message: "Invalid request body",
				issues,
			},
		},
		400
	);

const jsonContentTypeRegex = /^application\/([a-z0-9-.]+\+)?json(\s*;\s*[a-zA-Z0-9-]+=([^;]+))*$/i;

const echoJsonErrorMiddleware = createMiddleware(async (c, next) => {
	const contentType = c.req.header("Content-Type");
	if (contentType && jsonContentTypeRegex.test(contentType)) {
		try {
			JSON.parse(await c.req.text());
		} catch {
			return echoValidationError(c, [
				{
					path: [],
					code: "invalid_json",
					message: "Malformed JSON in request body",
				},
			]);
		}
	}

	await next();
});

const echoSchema = z.object({
	message: z.string().min(1),
});

const routes = new Hono()
	.get("/health", (c) => {
		return c.json({
			status: "ok",
			timestamp: new Date().toISOString(),
			uptime: process.uptime(),
			environment: process.env.NODE_ENV || "development",
		});
	})
	.post(
		"/echo",
		echoJsonErrorMiddleware,
		zValidator("json", echoSchema, (result, c) => {
			if (!result.success) {
				return echoValidationError(
					c,
					result.error.issues.map((issue) => ({
						path: issue.path.map(String),
						code: issue.code,
						message: issue.message,
					}))
				);
			}
		}),
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
