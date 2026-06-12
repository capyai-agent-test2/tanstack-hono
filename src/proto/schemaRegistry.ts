import { modelRouteFields } from "./modelRouteContract";

export const schemaRegistry = {
	ModelRoute: modelRouteFields,
};

export const getSchemaFieldTags = (schemaName: keyof typeof schemaRegistry) =>
	schemaRegistry[schemaName].map((field) => field.tag);
