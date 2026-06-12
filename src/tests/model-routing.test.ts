import { expect, test } from "vite-plus/test";

import { chooseModelRoute, laneForWorkspace } from "../model-routing";

test("chooses a route with a reason", () => {
	const decision = chooseModelRoute("preview review context");

	expect(decision.reason).toContain(decision.route.routeId);
});

test("maps qa workspaces to preview lane", () => {
	expect(laneForWorkspace("qa-two-lane")).toBe("preview");
});
