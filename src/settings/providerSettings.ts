export type ProviderSetting = {
	key: string;
	displayName: string;
	timeoutMs: number;
};

export const providerSettings: readonly ProviderSetting[] = [
	{ key: "openai", displayName: "OpenAI", timeoutMs: 15000 },
	{ key: "anthropic", displayName: "Anthropic", timeoutMs: 20000 },
	{ key: "local", displayName: "Local Fixture", timeoutMs: 5000 },
];
