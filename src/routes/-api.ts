import { zValidator } from "@hono/zod-validator";
import { Hono } from "hono";
import { z } from "zod";

export type FounderHealthDomain = "growth" | "activation" | "retention" | "reliability";

export interface FounderHealthMetric {
	label: string;
	value: string;
	context: string;
}

export interface FounderHealthDomainInsight {
	domain: FounderHealthDomain;
	score: number;
	trend: "accelerating" | "steady" | "watch" | "recovering";
	headline: string;
	keyMetrics: FounderHealthMetric[];
	whyItMatters: string;
	recommendedAction: string;
}

export interface FounderHealthResponse {
	asOf: string;
	companyStage: string;
	audience: string;
	overallScore: number;
	status: "board-ready" | "needs-attention" | "at-risk";
	verdict: string;
	executiveSummary: string;
	domains: FounderHealthDomainInsight[];
	nextBoardUpdate: {
		title: string;
		focus: string;
	};
	generatedBy: string;
}

/**
 * Deterministic sales-demo snapshot for an executive Founder Health readout.
 * Values are curated example data only and intentionally avoid runtime inputs.
 */
export const founderHealthDemo: FounderHealthResponse = {
	asOf: "2026-05-31T00:00:00.000Z",
	companyStage: "Series A SaaS, sales-assisted motion",
	audience: "Founder, executive team, and board observers",
	overallScore: 82,
	status: "board-ready",
	verdict: "Healthy with focused activation and retention work to unlock the next growth tier.",
	executiveSummary:
		"Pipeline quality and platform reliability are strong enough for a confident board narrative, while activation depth and logo retention deserve targeted founder attention this month.",
	domains: [
		{
			domain: "growth",
			score: 86,
			trend: "accelerating",
			headline: "Enterprise pipeline is expanding with disciplined payback.",
			keyMetrics: [
				{
					label: "Qualified pipeline",
					value: "$4.8M",
					context: "+18% quarter over quarter from target accounts",
				},
				{
					label: "CAC payback",
					value: "11.4 months",
					context: "inside the 12-month operating target",
				},
			],
			whyItMatters:
				"Efficient pipeline creation gives the founder room to invest without masking weak conversion quality.",
			recommendedAction:
				"Keep founder-led selling concentrated on the top 15 strategic opportunities and document repeatable discovery patterns for the sales team.",
		},
		{
			domain: "activation",
			score: 76,
			trend: "watch",
			headline: "New customers reach first value, but expansion-ready setup is inconsistent.",
			keyMetrics: [
				{
					label: "Time to first value",
					value: "6.2 days",
					context: "beating the 7-day onboarding goal",
				},
				{
					label: "Core workflow adoption",
					value: "68%",
					context: "12 points below best-fit customer benchmark",
				},
			],
			whyItMatters:
				"Activation is the earliest signal that customer promises are translating into habit-forming product value.",
			recommendedAction:
				"Add a 30-minute executive onboarding checkpoint for every strategic account before handoff to customer success.",
		},
		{
			domain: "retention",
			score: 79,
			trend: "steady",
			headline: "Revenue retention is solid, but three mid-market renewals need intervention.",
			keyMetrics: [
				{
					label: "Net revenue retention",
					value: "113%",
					context: "driven by expansion in healthcare and fintech cohorts",
				},
				{
					label: "At-risk ARR",
					value: "$420K",
					context: "concentrated in accounts with low executive engagement",
				},
			],
			whyItMatters:
				"Retention quality determines whether growth compounds or resets with every new selling cycle.",
			recommendedAction:
				"Schedule founder-to-founder renewal calls for the three flagged accounts and align each on a measurable 90-day success plan.",
		},
		{
			domain: "reliability",
			score: 88,
			trend: "recovering",
			headline: "Reliability is now a proof point after a clean incident-reduction cycle.",
			keyMetrics: [
				{
					label: "Production uptime",
					value: "99.96%",
					context: "measured against customer-facing API availability",
				},
				{
					label: "Severity-one incidents",
					value: "0",
					context: "for the last completed reporting month",
				},
			],
			whyItMatters:
				"Reliable delivery protects executive trust and supports larger commitments from regulated buyers.",
			recommendedAction:
				"Turn the incident-reduction work into a customer-facing reliability slide for late-stage enterprise deals.",
		},
	],
	nextBoardUpdate: {
		title: "Founder Health operating review",
		focus: "Activation depth, renewal saves, and enterprise pipeline conversion",
	},
	generatedBy: "Founder Health API demo fixture v1",
};

const routes = new Hono()
	.get("/health", (c) => {
		return c.json({
			status: "ok",
			timestamp: new Date().toISOString(),
			uptime: process.uptime(),
			environment: process.env.NODE_ENV || "development",
		});
	})
	.get("/founder-health", (c) => {
		return c.json(founderHealthDemo);
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
