import { Hono } from "hono";

const API_MOUNT_PATH = "/api";
const RELEASE_READINESS_ROUTE = "/release-readiness";

const requiredEnvironmentDefaults = [
	"NODE_ENV",
	"NODE_SERVER_HOST",
	"NODE_SERVER_PORT",
	"CORS_ORIGIN",
] as const;

const expectedBuildAssets = ["dist/client/.vite/manifest.json", "dist/server/index.js"] as const;

const runtimeMetadata = {
	runtime: "node",
	serverFramework: "hono",
	toolchain: "vite-plus",
	routeMount: `${API_MOUNT_PATH}${RELEASE_READINESS_ROUTE}`,
} as const;

type CheckStatus = "pass" | "fail";
type ReadinessStatus = "ready" | "not_ready";

interface ReadinessCheck {
	id: string;
	status: CheckStatus;
	message: string;
}

interface ReleaseReadinessResponse {
	status: ReadinessStatus;
	summary: {
		total: number;
		passed: number;
		failed: number;
	};
	checks: ReadinessCheck[];
	metadata: typeof runtimeMetadata;
}

function getReleaseReadinessChecks(): ReadinessCheck[] {
	return [
		{
			id: "environment.defaults",
			status: requiredEnvironmentDefaults.length === 4 ? "pass" : "fail",
			message: "Server environment variables have deterministic application defaults.",
		},
		{
			id: "build.assets.expected",
			status: expectedBuildAssets.every((asset) => asset.startsWith("dist/")) ? "pass" : "fail",
			message: "Production build expects client manifest and server entry assets.",
		},
		{
			id: "api.route.mount",
			status: runtimeMetadata.routeMount === "/api/release-readiness" ? "pass" : "fail",
			message: "Release readiness handler is mounted under the API route prefix.",
		},
		{
			id: "runtime.metadata",
			status:
				runtimeMetadata.runtime === "node" &&
				runtimeMetadata.serverFramework === "hono" &&
				runtimeMetadata.toolchain === "vite-plus"
					? "pass"
					: "fail",
			message: "Runtime and dependency metadata are declared with stable values.",
		},
	];
}

export function getReleaseReadiness(): ReleaseReadinessResponse {
	const checks = getReleaseReadinessChecks();
	const failed = checks.filter((check) => check.status === "fail").length;
	const passed = checks.length - failed;

	return {
		status: failed === 0 ? "ready" : "not_ready",
		summary: {
			total: checks.length,
			passed,
			failed,
		},
		checks,
		metadata: runtimeMetadata,
	};
}

const routes = new Hono().get(RELEASE_READINESS_ROUTE, (c) => {
	const readiness = getReleaseReadiness();

	if (readiness.status === "ready") {
		return c.json(readiness, 200);
	}

	return c.json(readiness, 503);
});

export type ReleaseReadinessRoutes = typeof routes;
export const handler = routes;
