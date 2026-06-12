export type ProtoField = {
	name: string;
	tag: number;
	type: string;
};

export const modelRouteFields: readonly ProtoField[] = [
	{ name: "route_id", tag: 1, type: "string" },
	{ name: "provider_key", tag: 2, type: "string" },
	{ name: "lane", tag: 3, type: "string" },
];
