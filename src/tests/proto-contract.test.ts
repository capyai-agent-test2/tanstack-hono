import { expect, test } from "vite-plus/test";

import { modelRouteFields } from "../proto";

test("declares route_id as the first fixture field", () => {
	expect(modelRouteFields[0]).toEqual({ name: "route_id", tag: 1, type: "string" });
});
