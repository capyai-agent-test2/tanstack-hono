/** String, date, and array utility fixtures for QA supersede testing. */
export type SortDirection = "asc" | "desc";

/** Trims text and collapses repeated whitespace into single spaces. */
export function normalizeWhitespace(value: string): string {
	return value.trim().replace(/\s+/g, " ");
}

/** Converts a phrase to kebab-case using simple ASCII word boundaries. */
export function toKebabCase(value: string): string {
	return normalizeWhitespace(value)
		.replace(/([a-z0-9])([A-Z])/g, "$1-$2")
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "");
}

/** Converts a phrase to snake_case using simple ASCII word boundaries. */
export function toSnakeCase(value: string): string {
	return toKebabCase(value).replace(/-/g, "_");
}

/** Capitalizes the first visible character without changing the rest. */
export function capitalize(value: string): string {
	return value.length === 0 ? value : value.charAt(0).toUpperCase() + value.slice(1);
}

/** Lowercases the first visible character without changing the rest. */
export function uncapitalize(value: string): string {
	return value.length === 0 ? value : value.charAt(0).toLowerCase() + value.slice(1);
}

/** Truncates text to a maximum length and appends an ellipsis when needed. */
export function truncate(value: string, maxLength: number, suffix = "…"): string {
	if (maxLength <= suffix.length) return suffix.slice(0, Math.max(0, maxLength));
	return value.length > maxLength ? `${value.slice(0, maxLength - suffix.length)}${suffix}` : value;
}

/** Returns true when a string is null, undefined, empty, or whitespace only. */
export function isBlank(value: string | null | undefined): boolean {
	return value == null || value.trim().length === 0;
}

/** Counts words after normalizing whitespace. */
export function countWords(value: string): number {
	const normalized = normalizeWhitespace(value);
	return normalized.length === 0 ? 0 : normalized.split(" ").length;
}

/** Pads a number with leading zeroes for display identifiers. */
export function padNumber(value: number, width: number): string {
	return String(Math.trunc(value)).padStart(width, "0");
}

/** Escapes a small set of HTML-sensitive characters. */
export function escapeHtml(value: string): string {
	return value
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;")
		.replace(/'/g, "&#39;");
}

/** Parses an ISO date string and returns null for invalid values. */
export function parseIsoDate(value: string): Date | null {
	const date = new Date(value);
	return Number.isNaN(date.getTime()) ? null : date;
}

/** Formats a date as YYYY-MM-DD in UTC. */
export function formatDateKey(date: Date): string {
	return `${date.getUTCFullYear()}-${padNumber(date.getUTCMonth() + 1, 2)}-${padNumber(date.getUTCDate(), 2)}`;
}

/** Adds a number of days to a date without mutating the original. */
export function addDays(date: Date, days: number): Date {
	const next = new Date(date);
	next.setUTCDate(next.getUTCDate() + days);
	return next;
}

/** Returns the inclusive day difference between two UTC dates. */
export function diffDays(start: Date, end: Date): number {
	const day = 24 * 60 * 60 * 1000;
	return Math.round(
		(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), end.getUTCDate()) -
			Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate())) /
			day
	);
}

/** Returns true when two dates fall on the same UTC day. */
export function isSameUtcDay(left: Date, right: Date): boolean {
	return formatDateKey(left) === formatDateKey(right);
}

/** Clamps a date between optional minimum and maximum bounds. */
export function clampDate(date: Date, min?: Date, max?: Date): Date {
	const time = date.getTime();
	if (min && time < min.getTime()) return new Date(min);
	if (max && time > max.getTime()) return new Date(max);
	return new Date(date);
}

/** Builds an array of UTC date keys between two dates, inclusive. */
export function dateRangeKeys(start: Date, end: Date): string[] {
	const keys: string[] = [];
	const direction = start <= end ? 1 : -1;
	for (let cursor = new Date(start); ; cursor = addDays(cursor, direction)) {
		keys.push(formatDateKey(cursor));
		if (isSameUtcDay(cursor, end)) break;
	}
	return keys;
}

/** Returns a shallow array copy with duplicate primitive values removed. */
export function uniqueValues<T extends string | number | boolean>(values: T[]): T[] {
	return Array.from(new Set(values));
}

/** Splits an array into chunks of a requested positive size. */
export function chunkArray<T>(values: T[], size: number): T[][] {
	const chunkSize = Math.max(1, Math.floor(size));
	const chunks: T[][] = [];
	for (let index = 0; index < values.length; index += chunkSize) {
		chunks.push(values.slice(index, index + chunkSize));
	}
	return chunks;
}

/** Returns the first item that is not null or undefined. */
export function firstDefined<T>(values: Array<T | null | undefined>): T | undefined {
	return values.find((value): value is T => value != null);
}

/** Groups array values by a derived string key. */
export function groupBy<T>(values: T[], getKey: (value: T) => string): Record<string, T[]> {
	return values.reduce<Record<string, T[]>>((groups, value) => {
		const key = getKey(value);
		groups[key] ??= [];
		groups[key].push(value);
		return groups;
	}, {});
}

/** Sorts values by a derived comparable key without mutating the input array. */
export function sortBy<T>(
	values: T[],
	getKey: (value: T) => string | number,
	direction: SortDirection = "asc"
): T[] {
	const multiplier = direction === "asc" ? 1 : -1;
	return [...values].sort((left, right) => {
		const leftKey = getKey(left);
		const rightKey = getKey(right);
		if (leftKey < rightKey) return -1 * multiplier;
		if (leftKey > rightKey) return 1 * multiplier;
		return 0;
	});
}

/** Partitions values into matching and non-matching arrays. */
export function partition<T>(values: T[], predicate: (value: T) => boolean): [T[], T[]] {
	const matching: T[] = [];
	const remaining: T[] = [];
	for (const value of values) {
		if (predicate(value)) matching.push(value);
		else remaining.push(value);
	}
	return [matching, remaining];
}

/** Returns the last element in an array, or undefined for an empty array. */
export function last<T>(values: T[]): T | undefined {
	return values.length === 0 ? undefined : values[values.length - 1];
}

/** Moves an array element from one index to another without mutating input. */
export function moveItem<T>(values: T[], fromIndex: number, toIndex: number): T[] {
	const copy = [...values];
	const [item] = copy.splice(fromIndex, 1);
	if (item === undefined) return values.slice();
	copy.splice(toIndex, 0, item);
	return copy;
}

/** Sums numeric values derived from each item. */
export function sumBy<T>(values: T[], getValue: (value: T) => number): number {
	return values.reduce((total, value) => total + getValue(value), 0);
}

/** Calculates an average for derived numeric values. */
export function averageBy<T>(values: T[], getValue: (value: T) => number): number {
	return values.length === 0 ? 0 : sumBy(values, getValue) / values.length;
}

/** Creates an integer range from start to end, inclusive. */
export function range(start: number, end: number): number[] {
	const direction = start <= end ? 1 : -1;
	const values: number[] = [];
	for (let value = start; direction > 0 ? value <= end : value >= end; value += direction) {
		values.push(value);
	}
	return values;
}

/** Clamps a number into an inclusive range. */
export function clampNumber(value: number, min: number, max: number): number {
	return Math.min(max, Math.max(min, value));
}

/** Rounds a number to a fixed number of decimal places. */
export function roundTo(value: number, precision: number): number {
	const scale = 10 ** Math.max(0, precision);
	return Math.round(value * scale) / scale;
}

/** Converts a fraction to a percentage string. */
export function formatPercent(value: number, precision = 0): string {
	return `${roundTo(value * 100, precision)}%`;
}

/** Safely parses JSON and returns a fallback for invalid input. */
export function parseJsonOr<T>(value: string, fallback: T): T {
	try {
		return JSON.parse(value) as T;
	} catch {
		return fallback;
	}
}

/** Delays for the requested number of milliseconds. */
export function delay(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Retries an async operation a fixed number of times. */
export async function retry<T>(operation: () => Promise<T>, attempts: number): Promise<T> {
	let lastError: unknown;
	for (let attempt = 0; attempt < Math.max(1, attempts); attempt += 1) {
		try {
			return await operation();
		} catch (error) {
			lastError = error;
		}
	}
	throw lastError;
}

/** Creates a stable object key by sorting enumerable keys. */
export function stableObjectKey(value: Record<string, unknown>): string {
	return JSON.stringify(
		Object.keys(value)
			.sort()
			.map((key) => [key, value[key]])
	);
}

/** Picks a subset of object properties by key. */
export function pick<T extends Record<string, unknown>, K extends keyof T>(
	value: T,
	keys: K[]
): Pick<T, K> {
	const result = {} as Pick<T, K>;
	for (const key of keys) result[key] = value[key];
	return result;
}

/** Omits a subset of object properties by key. */
export function omit<T extends Record<string, unknown>, K extends keyof T>(
	value: T,
	keys: K[]
): Omit<T, K> {
	const blocked = new Set<keyof T>(keys);
	const result = {} as Omit<T, K>;
	for (const key of Object.keys(value) as Array<keyof T>) {
		if (!blocked.has(key))
			result[key as Exclude<keyof T, K>] = value[key] as Omit<T, K>[Exclude<keyof T, K>];
	}
	return result;
}

/** Creates a deterministic label from a prefix and numeric id. */
export function makeFixtureLabel(prefix: string, id: number): string {
	return `${toKebabCase(prefix)}-${padNumber(id, 4)}`;
}
