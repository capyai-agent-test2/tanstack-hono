export const openaiProvider = {
	key: "openai",
	summarize: (input: string) => `openai:${input.slice(0, 16)}`,
};
