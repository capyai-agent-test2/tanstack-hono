export const anthropicProvider = {
	key: "anthropic",
	summarize: (input: string) => `anthropic:${input.slice(0, 16)}`,
};
