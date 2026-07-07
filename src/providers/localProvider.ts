export const localProvider = {
	key: "local",
	summarize: (input: string) => `local:${input.slice(0, 16)}`,
};
