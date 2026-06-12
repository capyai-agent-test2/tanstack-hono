export type ReviewContextSettings = {
	includeBaseContext: boolean;
	includeIncrementalDiff: boolean;
	maxContextFiles: number;
};

export const reviewContextSettings: ReviewContextSettings = {
	includeBaseContext: true,
	includeIncrementalDiff: true,
	maxContextFiles: 20,
};
