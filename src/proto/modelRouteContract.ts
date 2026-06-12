export type ProtoField = {
	name: string;
	tag: number;
	type: string;
};

export const modelRouteFields: readonly ProtoField[] = [
	{ name: "route_id", tag: 11, type: "string" },
	{ name: "provider_key", tag: 20, type: "string" },
	{ name: "lane", tag: 30, type: "string" },
];
