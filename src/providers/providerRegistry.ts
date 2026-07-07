import { anthropicProvider } from "./anthropicProvider";
import { localProvider } from "./localProvider";
import { openaiProvider } from "./openaiProvider";

export const providerRegistry = {
	[anthropicProvider.key]: anthropicProvider,
	[localProvider.key]: localProvider,
	[openaiProvider.key]: openaiProvider,
};

export const getProvider = (key: keyof typeof providerRegistry) => providerRegistry[key];
